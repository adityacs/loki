package queryrange

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/common/model"
	"github.com/uber/jaeger-client-go"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
)

// ResultsCacheConfig is the config for the results cache.
type ResultsCacheConfig struct {
	CacheConfig cache.Config `yaml:"cache"`

	// TODO: remove this, when removed in cortex
	LegacyMaxCacheFreshness time.Duration `yaml:"max_freshness" doc:"hidden"`
}

type CacheGenNumberLoader interface {
	GetResultsCacheGenNumber(userID string) string
}

// CacheSplitter generates cache keys. This is a useful interface for downstream
// consumers who wish to implement their own strategies.
type CacheSplitter interface {
	GenerateCacheKey(userID string, r queryrange.Request) string
}

type resultsCache struct {
	logger   log.Logger
	cfg      ResultsCacheConfig
	next     queryrange.Handler
	cache    cache.Cache
	limits   Limits
	splitter CacheSplitter
	merger   queryrange.Merger
}

// NewResultsCacheMiddleware creates results cache middleware from config.
// The middleware cache result using a unique cache key for a given request (step,query,user) and interval.
// The cache assumes that each request length (end-start) is below or equal the interval.
// Each request starting from within the same interval will hit the same cache entry.
// If the cache doesn't have the entire duration of the request cached, it will query the uncached parts and append them to the cache entries.
// see `generateKey`.
func NewResultsCacheMiddleware(
	logger log.Logger,
	cfg ResultsCacheConfig,
	splitter CacheSplitter,
	limits Limits,
	merger queryrange.Merger,
) (queryrange.Middleware, cache.Cache, error) {
	c, err := cache.New(cfg.CacheConfig)
	if err != nil {
		return nil, nil, err
	}

	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return &resultsCache{
			logger:   logger,
			cfg:      cfg,
			next:     next,
			cache:    c,
			limits:   limits,
			merger:   merger,
			splitter: splitter,
		}
	}), c, nil
}

func (s resultsCache) Do(ctx context.Context, r queryrange.Request) (queryrange.Response, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	var (
		key      = s.splitter.GenerateCacheKey(userID, r)
		extents  []queryrange.Extent
		response queryrange.Response
	)

	// check if cache freshness value is provided in legacy config
	maxCacheFreshness := s.cfg.LegacyMaxCacheFreshness
	if maxCacheFreshness == time.Duration(0) {
		maxCacheFreshness = s.limits.MaxCacheFreshness(userID)
	}
	maxCacheTime := int64(model.Now().Add(-maxCacheFreshness))
	if r.GetStart() > maxCacheTime {
		return s.next.Do(ctx, r)
	}

	cached, ok := s.get(ctx, key)
	if ok {
		response, extents, err = s.handleHit(ctx, r, cached)
	} else {
		response, extents, err = s.handleMiss(ctx, r)
	}

	if err == nil && len(extents) > 0 {
		// extents, err := s.filterRecentExtents(r, maxCacheFreshness, extents)
		// if err != nil {
		// 	return nil, err
		// }
		s.put(ctx, key, extents)
	}

	return response, err
}

func (s resultsCache) handleHit(ctx context.Context, r queryrange.Request, extents []queryrange.Extent) (queryrange.Response, []queryrange.Extent, error) {
	var (
		reqResps []queryrange.RequestResponse
		err      error
	)
	log, ctx := spanlogger.New(ctx, "handleHit")
	defer log.Finish()

	requests, responses, err := partition(r, extents)
	if err != nil {
		return nil, nil, err
	}
	if len(requests) == 0 {
		response, err := s.merger.MergeResponse(responses...)
		// No downstream requests so no need to write back to the cache.
		return response, nil, err
	}

	reqResps, err = queryrange.DoRequests(ctx, s.next, requests, s.limits)
	if err != nil {
		return nil, nil, err
	}

	for _, reqResp := range reqResps {
		responses = append(responses, reqResp.Response)
		extent, err := toExtent(ctx, reqResp.Request, reqResp.Response)
		if err != nil {
			return nil, nil, err
		}
		extents = append(extents, extent)
	}

	sort.Slice(extents, func(i, j int) bool {
		return extents[i].Start < extents[j].Start
	})

	response, err := s.merger.MergeResponse(responses...)
	return response, extents, err
}

// partition calculates the required requests to satisfy req given the cached data.
func partition(req queryrange.Request, extents []queryrange.Extent) ([]queryrange.Request, []queryrange.Response, error) {
	var requests []queryrange.Request
	var cachedResponses []queryrange.Response
	start := req.GetStart()

	for _, extent := range extents {
		// If there is no overlap, ignore this extent.
		if extent.GetEnd() < start || extent.Start > req.GetEnd() {
			continue
		}

		// if the request range is smaller than the extent range, create new request
		if extent.End > start && extent.Start < req.GetEnd() {
			r := req.WithStartEnd(start, req.GetEnd())
			requests = append(requests, r)
			continue
		}

		// If there is a bit missing at the front, make a request for that.
		if start < extent.Start {
			r := req.WithStartEnd(start, extent.Start)
			requests = append(requests, r)
		}
		res, err := toResponse(extent)
		if err != nil {
			return nil, nil, err
		}
		// extract the overlap from the cached extent.
		cachedResponses = append(cachedResponses, res)
		start = extent.End
	}

	if start < req.GetEnd() {
		r := req.WithStartEnd(start, req.GetEnd())
		requests = append(requests, r)
	}

	return requests, cachedResponses, nil
}

func (s resultsCache) handleMiss(ctx context.Context, r queryrange.Request) (queryrange.Response, []queryrange.Extent, error) {
	response, err := s.next.Do(ctx, r)
	if err != nil {
		return nil, nil, err
	}

	extent, err := toExtent(ctx, r, response)
	if err != nil {
		return nil, nil, err
	}

	extents := []queryrange.Extent{
		extent,
	}
	return response, extents, nil
}

func toExtent(ctx context.Context, req queryrange.Request, res queryrange.Response) (queryrange.Extent, error) {
	any, err := types.MarshalAny(res)
	if err != nil {
		return queryrange.Extent{}, err
	}
	return queryrange.Extent{
		Start:    req.GetStart(),
		End:      req.GetEnd(),
		Response: any,
		TraceId:  jaegerTraceID(ctx),
	}, nil
}

func jaegerTraceID(ctx context.Context) string {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return ""
	}

	spanContext, ok := span.Context().(jaeger.SpanContext)
	if !ok {
		return ""
	}

	return spanContext.TraceID().String()
}

func (s resultsCache) get(ctx context.Context, key string) ([]queryrange.Extent, bool) {
	found, bufs, _ := s.cache.Fetch(ctx, []string{cache.HashKey(key)})
	if len(found) != 1 {
		return nil, false
	}

	var resp queryrange.CachedResponse
	sp, _ := opentracing.StartSpanFromContext(ctx, "unmarshal-extent")
	defer sp.Finish()

	sp.LogFields(otlog.Int("bytes", len(bufs[0])))

	if err := proto.Unmarshal(bufs[0], &resp); err != nil {
		level.Error(s.logger).Log("msg", "error unmarshalling cached value", "err", err)
		sp.LogFields(otlog.Error(err))
		return nil, false
	}

	if resp.Key != key {
		return nil, false
	}

	// Refreshes the cache if it contains an old proto schema.
	for _, e := range resp.Extents {
		if e.Response == nil {
			return nil, false
		}
	}

	return resp.Extents, true
}

func (s resultsCache) put(ctx context.Context, key string, extents []queryrange.Extent) {
	buf, err := proto.Marshal(&queryrange.CachedResponse{
		Key:     key,
		Extents: extents,
	})
	if err != nil {
		level.Error(s.logger).Log("msg", "error marshalling cached value", "err", err)
		return
	}

	s.cache.Store(ctx, []string{cache.HashKey(key)}, [][]byte{buf})
}

func toResponse(e queryrange.Extent) (queryrange.Response, error) {
	msg, err := types.EmptyAny(e.Response)
	if err != nil {
		return nil, err
	}

	if err := types.UnmarshalAny(e.Response, msg); err != nil {
		return nil, err
	}

	resp, ok := msg.(queryrange.Response)
	if !ok {
		return nil, fmt.Errorf("bad cached type")
	}
	return resp, nil
}
