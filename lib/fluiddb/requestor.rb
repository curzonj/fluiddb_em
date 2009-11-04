module FluidDB
  module Requestor
    def connection
      FluidDB.http
    end

    def request(*args)
      req = connection.request(*args)
    end

    def get(uri, params=nil, &block)
      request(:get, uri, params, &block)
    end

    include Loggable
    include Memcache if defined? MemCache
  end
end

