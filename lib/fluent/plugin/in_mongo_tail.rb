module Fluent
  class MongoTailInput < Input
    Plugin.register_input('mongo_tail', self)

    require 'fluent/plugin/mongo_util'
    include MongoUtil

    config_param :database, :string, :default => nil
    config_param :collection, :string
    config_param :host, :string, :default => 'localhost'
    config_param :port, :integer, :default => 27017
    config_param :wait_time, :integer, :default => 1
    config_param :url, :string, :default => nil

    config_param :tag, :string, :default => nil
    config_param :tag_key, :string, :default => nil
    config_param :time_key, :string, :default => nil
    config_param :time_format, :string, :default => nil

    # To store last ObjectID
    config_param :id_store_file, :string, :default => nil

    # SSL connection
    config_param :ssl, :bool, :default => false

    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    def initialize
      super
      require 'mongo'
      require 'bson'

      @connection_options = {}
    end

    def configure(conf)
      super

      if !@tag and !@tag_key
        raise ConfigError, "'tag' or 'tag_key' option is required on mongo_tail input"
      end

      if @database && @url
        raise ConfigError, "Both 'database' and 'url' can not be set"
      end

      if !@database && !@url
        raise ConfigError, "One of 'database' or 'url' must be specified"
      end

      @last_id = @id_store_file ? get_last_id : nil
      @connection_options[:ssl] = @ssl

      $log.debug "Setup mongo_tail configuration: mode = #{@id_store_file ? 'persistent' : 'non-persistent'}"
    end

    def start
      super
      @file = get_id_store_file if @id_store_file
      @client = get_capped_collection
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      if @id_store_file
        save_last_id
        @file.close
      end

      @stop = true
      @thread.join
      @client.db.connection.close
      super
    end

    def run
      loop {
        cursor = Mongo::Cursor.new(@client, cursor_conf)
        begin
          loop {
            return if @stop
            
            cursor = Mongo::Cursor.new(@client, cursor_conf) unless cursor.alive?
            if doc = cursor.next_document
              process_document(doc)
            else
              sleep @wait_time
            end
          }
        rescue
          # ignore Mongo::OperationFailuer at CURSOR_NOT_FOUND
        end
      }
    end

    private

    def get_capped_collection
      begin
        client = get_client
        unless client.database.collection_names.include?(@collection)
          raise ConfigError, "'#{client.database.name}.#{@collection}' not found: node = #{node_string}"
        end
        collection = client[@collection]
        unless collection.capped?
          raise ConfigError, "'#{client.database.name}.#{@collection}' is not capped: node = #{node_string}"
        end
        collection
      rescue Mongo::Auth::Unauthorized => e
        log.fatal "#{e.class}: #{e.message}"
        exit!
      rescue Mongo::Error::OperationFailure => e
        log.fatal "#{e.class}: #{e.message}"
        exit!
      end
    end

    def get_client
      case
      when @database
        options = {}
        options.merge(@connection_options)
        options[:user] = @user if @user
        options[:password] = @password if @password
        Mongo::Client.new(["#{@host}:#{@port}"], options)
      when @url
        Mongo::Client.new(@url)
      end
    end

    def database_name
      case
      when @database
        @database
      when @url
        Mongo::URIParser.new(@url).db_name
      end
    end

    def node_string
      case
      when @database
        "#{@host}:#{@port}"
      when @url
        @url
      end
    end

    def process_document(doc)
      time = if @time_key
               t = doc.delete(@time_key)
               t.nil? ? Engine.now : t.to_i
             else
               Engine.now
             end
      tag = if @tag_key
              t = doc.delete(@tag_key)
              t.nil? ? 'mongo.missing_tag' : t
            else
              @tag
            end
      if id = doc.delete('_id')
        @last_id = id.to_s
        doc['_id_str'] = @last_id
        save_last_id if @id_store_file
      end

      # Should use MultiEventStream?
      router.emit(tag, time, doc)
    end

    def cursor_conf
      conf = {}
      conf[:tailable] = true
      conf[:selector] = {'_id' => {'$gt' => BSON::ObjectId(@last_id)}} if @last_id
      conf
    end

    # following methods are used when id_store_file is true

    def get_id_store_file
      file = File.open(@id_store_file, 'w')
      file.sync
      file
    end

    def get_last_id
      if File.exist?(@id_store_file)
        BSON::ObjectId(File.read(@id_store_file)).to_s rescue nil
      else
        nil
      end
    end

    def save_last_id
      @file.pos = 0
      @file.write(@last_id)
    end
  end
end
