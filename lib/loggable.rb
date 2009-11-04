require 'logger'

module Loggable
  def self.log=(global)
    @log = global
  end

  def self.log
    @log || Rails.log rescue Logger.new(STDERR)
  end

  def self.included(base)
    base.class_eval do
      def log=(l)
        @log = l
      end

      def log
        @log ||= Loggable.log
      end
    end
  end

  def log
    self.class.log
  end
end
