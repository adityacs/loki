package stages

import (
	"fmt"
	"net"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/mitchellh/mapstructure"
	"github.com/oschwald/geoip2-golang"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

const (
	ErrEmptyGeoIPStageConfig       = "geoip stage config cannot be empty"
	ErrEmptyDBPathGeoIPStageConfig = "db path cannot be empty"
	ErrEmptySourceGeoIPStageConfig = "source cannot be empty"
	ErrEmptyDBTypeGeoIPStageConfig = "db type should be either city or asn"
)

type GeoIPFields int

const (
	CITY_NAME GeoIPFields = iota
	COUNTRY_NAME
	CONTINENT_NAME
	CONTINENT_CODE
	LOCATION
	POSTAL_CODE
	TIMEZONE
	SUBDIVISION_NAME
	SUBDIVISION_CODE
)

var fields = map[GeoIPFields]string{
	CITY_NAME:        "geoip_city_name",
	COUNTRY_NAME:     "geoip_country_name",
	CONTINENT_NAME:   "geoip_continet_name",
	CONTINENT_CODE:   "geoip_continent_code",
	LOCATION:         "geoip_location",
	POSTAL_CODE:      "geoip_postal_code",
	TIMEZONE:         "geoip_timezone",
	SUBDIVISION_NAME: "geoip_subdivision_name",
	SUBDIVISION_CODE: "geoip_subdivision_code",
}

// GeoIPConfig represents GeoIP stage config
type GeoIPConfig struct {
	DB     string `mapstructure:"db"`
	Source string `mapstructure:"source"`
	DBType string `mapstructure:"db_type"`
}

func validateGeoIPConfig(c *GeoIPConfig) error {
	if c == nil {
		return errors.New(ErrEmptyGeoIPStageConfig)
	}

	if c.DB == "" {
		return errors.New(ErrEmptyDBPathGeoIPStageConfig)
	}

	if c.Source == "" {
		return errors.New(ErrEmptySourceGeoIPStageConfig)
	}

	if c.DBType == "" {
		return errors.New(ErrEmptyDBTypeGeoIPStageConfig)
	}

	return nil
}

func newGeoIPStage(logger log.Logger, configs interface{}) (Stage, error) {
	cfgs := &GeoIPConfig{}
	err := mapstructure.Decode(configs, cfgs)
	if err != nil {
		return nil, err
	}

	err = validateGeoIPConfig(cfgs)
	if err != nil {
		return nil, err
	}

	db, err := geoip2.Open(cfgs.DB)
	if err != nil {
		return nil, err
	}

	return toStage(&geoIPStage{
		db:     db,
		logger: logger,
		cfgs:   cfgs,
	}), nil
}

type geoIPStage struct {
	logger log.Logger
	db     *geoip2.Reader
	cfgs   *GeoIPConfig
}

// Process implements Stage
func (g *geoIPStage) Process(labels model.LabelSet, extracted map[string]interface{}, t *time.Time, entry *string) {
	ip := net.ParseIP(g.cfgs.Source)
	switch g.cfgs.DBType {
	case "city":
		record, err := g.db.City(ip)
		if err != nil {
			if Debug {
				level.Debug(g.logger).Log("msg", "unable to get City record for the ip", "err", err, "ip", ip)
			}
		}
		g.populateLabelsWithCityData(labels, record)
	case "asn":
		record, err := g.db.ASN(ip)
		if err != nil {
			if Debug {
				level.Debug(g.logger).Log("msg", "unable to get ASN record for the ip", "err", err, "ip", ip)
			}
		}
		g.populateLabelsWithASNData(labels, record)
	default:
		level.Error(g.logger).Log("msg", "unknown database type")
	}
}

// Name implements Stage
func (g *geoIPStage) Name() string {
	return StageTypeGeoIP
}

// Close implements Stage
func (g *geoIPStage) Close() {
	if err := g.db.Close(); err != nil {
		level.Error(g.logger).Log("msg", "error while closing geoip db", "err", err)
	}
}

func (g *geoIPStage) populateLabelsWithCityData(labels model.LabelSet, record *geoip2.City) {
	for field, label := range fields {
		switch field {
		case CITY_NAME:
			cityName := record.City.Names["en"]
			if cityName != "" {
				labels[model.LabelName(label)] = model.LabelValue(cityName)
			}
		case COUNTRY_NAME:
			contryName := record.Country.Names["en"]
			if contryName != "" {
				labels[model.LabelName(label)] = model.LabelValue(contryName)
			}
		case CONTINENT_NAME:
			continentName := record.Continent.Names["en"]
			if continentName != "" {
				labels[model.LabelName(label)] = model.LabelValue(continentName)
			}
		case CONTINENT_CODE:
			continentCode := record.Continent.Code
			if continentCode != "" {
				labels[model.LabelName(label)] = model.LabelValue(continentCode)
			}
		case POSTAL_CODE:
			postalCode := record.Postal.Code
			if postalCode != "" {
				labels[model.LabelName(label)] = model.LabelValue(postalCode)
			}
		case TIMEZONE:
			timezone := record.Location.TimeZone
			if timezone != "" {
				labels[model.LabelName(label)] = model.LabelValue(timezone)
			}
		case LOCATION:
			latitude := record.Location.Latitude
			longitude := record.Location.Longitude
			if latitude != 0 || longitude != 0 {
				labels[model.LabelName(fmt.Sprintf("%s_latitude", label))] = model.LabelValue(fmt.Sprint(latitude))
				labels[model.LabelName(fmt.Sprintf("%s_longitude", label))] = model.LabelValue(fmt.Sprint(longitude))
			}
		case SUBDIVISION_NAME:
			if len(record.Subdivisions) > 0 {
				// we get most specific subdivision https://dev.maxmind.com/release-note/most-specific-subdivision-attribute-added/
				subdivision_name := record.Subdivisions[len(record.Subdivisions)-1].Names["en"]
				if subdivision_name != "" {
					labels[model.LabelName(label)] = model.LabelValue(subdivision_name)
				}
			}
		case SUBDIVISION_CODE:
			if len(record.Subdivisions) > 0 {
				subdivision_code := record.Subdivisions[len(record.Subdivisions)-1].IsoCode
				if subdivision_code != "" {
					labels[model.LabelName(label)] = model.LabelValue(subdivision_code)
				}
			}
		default:
			level.Error(g.logger).Log("msg", "unknown geoip field")
		}
	}
}

func (g *geoIPStage) populateLabelsWithASNData(labels model.LabelSet, record *geoip2.ASN) {
	autonomous_system_number := record.AutonomousSystemNumber
	autonomous_system_organization := record.AutonomousSystemOrganization
	if autonomous_system_number != 0 {
		labels[model.LabelName("geoip_autonomous_system_number")] = model.LabelValue(fmt.Sprint(autonomous_system_number))
	}
	if autonomous_system_organization != "" {
		labels[model.LabelName("geoip_autonomous_system_organization")] = model.LabelValue(autonomous_system_organization)
	}
}
