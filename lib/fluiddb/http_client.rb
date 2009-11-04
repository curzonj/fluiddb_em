module FluidDB
  class HttpClient < EM::Protocols::HttpClient2
    include Loggable

    DEFAULT_HEADERS = {'Content-Type' => 'application/json', 'Accept' => '*/*'}
    METHODS = { :get => "GET", :put => "PUT", :post => "POST", :delete => "DELETE" }

    # conn = FluidDB::HttpClient.connect('http://sandbox.fluidinfo.com/', 'test', 'test')
    def self.connect(url, username, password)
      uri = URI.parse(url)

      conn = super(:host => uri.host, :port => uri.port, :ssl => (uri.scheme == 'https' ? true : false))
      conn.credentials username, password
      conn
    end

    # req = conn.request(:post, '/objects') {|status, json| p(json) }
    def request(method, uri, params=nil, payload=nil, headers={}, &block)
      uri = self.class.with_params(uri, params) if params.is_a?(Hash)
      verb = METHODS[method]
      args = { :uri => uri,
               :verb => verb,
               :headers => DEFAULT_HEADERS.merge(headers) }
      args[:body] = payload.to_json if payload

      req = super(args)
      req.callback do |response|
        if response.status != 200
          log.warn "(#{response.status}) #{uri} -- #{response.headers.inspect}"
        else
          log.debug "(#{response.status}) #{uri}"
        end
      end
      req.errback{ log.error "Failed #{verb} #{uri}" }

      if block_given?
        req.callback {|response| self.class.handle_json_response(response, &block) }
      end
      req
    end

    def credentials(username, password)
      @authorization = "Basic #{Base64.encode64("#{username}:#{password}")}"
    end

    class << self
      def with_params(uri, values)
        values = values.inject([]){|arr,arg| arr << arg.join("=")}.join("&")
        uri += "?#{values}" unless values.empty?
        URI.encode(uri)
      end

      def handle_json_response(response)
        body = if response.headers['content-type'] == ["application/json"]
          JSON.parse(response.content)
        elsif response.headers['content-type'] == ["application/vnd.fluiddb.value+json"]
          JSON.parse('[' + response.content + ']').first
        else
          response.content
        end

        yield(response.status, body, response.headers)
      end
    end
  end
end

class EM::Protocols::HttpClient2::Request
  BLOCK_SIZE = 8192

  def get?
    @args[:verb] == "GET"
  end

  def send_request
    r = [
      "#{@args[:verb]} #{@args[:uri]} HTTP/#{@args[:version] || "1.1"}\r\n",
      "Host: #{@args[:host_header] || "_"}\r\n"
    ]

    az = @args[:authorization] and az = "Authorization: #{az.strip}\r\n"
    r << az if az

    if @args.has_key?(:headers)
      @args[:headers].each do |header, value|
        r << "#{header}: #{value}\r\n"
      end
    end

    if (body = @args[:body])
      unless body.respond_to?(:size) &&
             body.respond_to?(:read)
        body = StringIO.new(body.to_s)
      end

      r << "Content-Length: #{body.size}\r\n"
    end

    r << "\r\n"

    @conn.send_data r.join
    if body
      while (data = body.read(BLOCK_SIZE)) do
        @conn.send_data data
      end
    end
  end
end
