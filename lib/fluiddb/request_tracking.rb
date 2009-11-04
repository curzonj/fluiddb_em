module FluidDB
  module RequestTracking
    def self.included(base)
      base.class_eval do
        alias request_without_tracking request
        alias request request_with_tracking
      end
    end

    def current_requests
      @@current_requests ||= {}
    end

    def request_with_tracking(method, uri, params=nil, payload=nil, headers={}, &block)
      req = FluidDB.http.request(method, uri, params, payload, headers)
      current_requests[uri] = req

      req.errback { current_requests.delete(uri) }
      req.callback do |response|
        current_requests.delete(uri)
        HttpClient.handle_json_response(response, &block)
      end

      req
    end
  end
end
