module FluidDB
  module Memcache
    class << self
      attr_writer :cache_ttl
      def cache_ttl
        @cache_ttl || 300
      end

      def included(base)
        base.class_eval do
          alias get_without_memcache get
          alias get get_with_memcache
        end
      end
    end

    # Caches requests in memcache
    def get_with_memcache(uri, params={}, &block)
      return get_without_memcache(uri, params, &block) unless defined? CACHE

      uri = HttpClient.with_params(uri, params)
      if (value = CACHE.get(uri))
        return if block.nil?

        if value == 'currently_running'
          if (respond_to?(:current_requests) && req = current_requests[uri])
            req.callback do |response|
              HttpClient.handle_json_response(response, &block)
            end
            req
          else
            cache_miss(uri)
          end
        else
          log.debug("Retrieved #{uri} from cache")
          block.call(*value)
        end
      else
        cache_miss(uri, &block)
      end
    end

    private
    def cache_miss(uri)
      CACHE.add(uri, 'currently_running', 10)
      req = get_without_memcache(uri) do |status, json|
        if status == 200
          CACHE.set(uri, [status, json], Memcache.cache_ttl)
        else
          CACHE.delete(uri)
        end

        yield(status, json) if block_given?
      end

      # Set the errback after because if the req is already failed
      # then it would get trapped in the list
      req.errback do
        CACHE.delete(uri)
      end
    end
  end
end
