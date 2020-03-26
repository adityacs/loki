# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require 'net/http'
require 'concurrent-edge'
require 'time'
require 'uri'
require 'json'

# An example output that does nothing.
class LogStash::Outputs::Loki < LogStash::Outputs::Base
  require 'logstash/outputs/loki/batch'
  require 'logstash/outputs/loki/entry'

  config_name "loki"

  desc 'Loki URL'
  config :url, :validate => :string,  :required => true

  desc 'BasicAuth credentials'
  config :username, :validate => :string, :required => false
  config :password, :validate => :string, secret: true, :required => false

  desc 'Client certificate'
  config :cert, :validate => :path, :required => false
  config :key, :validate => :path, :required => false

  desc 'TLS'
  config :ca_cert, :validate => :path, :required => false

  desc 'Loki Tenant ID'
  config :tenant_id, :validate => :string, :default => "fake", :required => false

  desc 'Maximum number of records to hold in a batch before pushing to Loki'
  config :batch_size, :validate => :number, :default => 10, :required => false

  desc 'Interval to wait before pushing a batch of records to Loki'
  config :batch_wait, :validate => :number, :default => 1, :required => false

  desc 'Array of label names to exclude from sending to Loki'
  config :exclude_labels, :validate => :array, :required => false 

  desc 'Extra labels to add to all log streams'
  config :external_labels, :valide => :hash,  :required => false


  public
  def register
    @uri = URI.parse(@url + '/loki/api/v1/push')
    unless @uri.is_a?(URI::HTTP) || @uri.is_a?(URI::HTTPS)
      raise LogStash::ConfigurationError, "url parameter must be valid HTTP, currently '#{@url}'"
    end

    @logger.info("New Loki output", :class => self.class.name)

    @Channel = Concurrent::Channel
    @batches = {}
    @entries = @Channel.new
    @quit = @Channel.new
    @exclude_labels += ["message", "@timestamp"] 

    if ssl_cert?
      load_ssl
      validate_ssl_key
    end

    @Channel.go{run()}
  end

  def ssl_cert?
    !@key.nil? && !@cert.nil?
  end

  def load_ssl
    @cert = OpenSSL::X509::Certificate.new(File.read(@cert)) if @cert
    @key = OpenSSL::PKey.read(File.read(@key)) if @key
  end

  def validate_ssl_key
    if !@key.is_a?(OpenSSL::PKey::RSA) && !@key.is_a?(OpenSSL::PKey::DSA)
      raise "Unsupported private key type #{key.class}"
    end
  end

  def ssl_opts(uri)
    opts = {
      use_ssl: uri.scheme == 'https'
    }

    if !@cert.nil? && !@key.nil?
      opts = opts.merge(
        verify_mode: OpenSSL::SSL::VERIFY_PEER,
        cert: @cert,
        key: @key
      )
    end

    unless @ca_cert.nil?
      opts = opts.merge(
        ca_file: @ca_cert
      )
    end
    opts
  end 

  def run()
    puts "Inside run"
	  min_wait_checkfrequency = 1/1000 #1 millisecond
	  max_wait_checkfrequency = @batch_wait / 10
	  if max_wait_checkfrequency < min_wait_checkfrequency
		  max_wait_checkfrequency = min_wait_checkfrequency
    end

    max_wait_check = Concurrent::Channel.tick(max_wait_checkfrequency)
    loop do
      puts "Inside loop"
      Concurrent::Channel.select do |s|
        s.take(@entries) { |e|
          puts "inside take entries"
          if !@batches.has_key?(e.tenant_id)
            @batches[e.tenant_id] = Batch.new(e)
            next
          end

          batch = @batches[e.tenant_id]
          line = e.entry['line']
          if batch.size_bytes_after(line) > @batch_size
            send(e.tenant_id, batch)
            @batches[e.tenant_id] = Batch.new(e)
            next
          end
          batch.add(e)
        }
        s.take(max_wait_check) {
          puts "inside take max wait check"
          # Send all batches whose max wait time has been reached
          @batches.each { |tenant_id, batch|
            if batch.age() < @batch_wait
              puts "Batch wait time is less"
              next
            end 
            puts "Batch wait time is crossed. Sending batch"
            send(tenant_id, batch)
            @batches.delete(tenant_id)
          }
        }
        s.take(@quit) {
          puts "Quiting"
          @quit << true
        }
        s.default do
          puts "I am just default"
          sleep(3)
        end 
      end
    end 
  end

  public
  def receive(event)
    puts "I am inside receive"
    labels = {}
    event_hash = event.to_hash
    lbls = extract_labels(event_hash, labels, "")
    data_labels = lbls.merge(@external_labels)

    entry_hash = {
      "ts" => event.get("@timestamp").to_i * (10**9) +  Time.new.nsec,
      "line" => event.get("message").to_s
    }

    @Channel.go do
      @entries << Entry.new(@tenant_id, data_labels, entry_hash)
    end 
  end

  def format_key(key)
    if key[0] == "@"
      key = key[1:]
      return key
  end 

  def extract_labels(event_hash, labels, parent_key)
    puts "I am inside extract_labels"
    event_hash.each{ |key,value|
      key = format_key(key)
      if !@exclude_labels.include?(key) && value.is_a?(Array)
        if value.is_a?(Hash)
          if parent_key != ""
            extract_labels(value, labels, parent_key + "_" + key)
          else  
            extract_labels(value, labels, key)
          end
        else
          if parent_key != ""
            labels[parent_key + "_" + key] = value.to_s
          else 
            labels[key] = value.to_s
          end
        end
      end
    }
    return labels
  end

  def close
    puts "Closing"
    @entries.close 
    @quit << "quit"
    puts @quit.take
  end

  def send(tenant_id, batch)
    puts "Sending Batch"
    payload = build_payload(batch)
    
    req = Net::HTTP::Post.new(
      @uri.request_uri
    )
    req.add_field('Content-Type', 'application/json')
    req.add_field('X-Scope-OrgID', tenant_id) if tenant_id
    req.basic_auth(@username, @password) if @username
    req.body = payload

    opts = ssl_opts(@uri)

    res = Net::HTTP.start(@uri.host, @uri.port, **opts) { |http| http.request(req) }
    unless res&.is_a?(Net::HTTPSuccess)
      res_summary = if res
                      "#{res.code} #{res.message} #{res.body}"
                    else
                      'res=nil'
                    end
      puts res_summary
    end 
  end

  def build_payload(batch)
    payload = {}
    payload['streams'] = []
    batch.streams.each { |labels, stream|
        stream_obj = get_stream_obj(stream)
        payload['streams'].push(stream_obj)
    }
    return payload.to_json
  end

  def get_stream_obj(steam)
    stream_obj = {} 
    stream_obj['stream'] = stream['labels']
    stream_obj['values'] = [] 
    values = []
    stream['entries'].each { |entry|
        values.push(entry['ts'].to_s)
        values.push(entry['line'])
    }
    stream_obj['values'].push(values)
    return stream_obj
  end 
end
