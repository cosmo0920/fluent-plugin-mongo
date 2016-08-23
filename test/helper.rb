require 'test/unit'
require 'fluent/test'
require 'mongo'
require 'fluent/plugin/out_mongo'
require 'fluent/plugin/out_mongo_replset'
require 'fluent/plugin/in_mongo_tail'

# for testing

def unused_port
  s = TCPServer.open(0)
  port = s.addr[1]
  s.close
  port
end

# for MongoDB

require 'mongo'
require 'open3'

MONGO_DB_DB = 'fluent_test'
MONGO_DB_PATH = File.join(File.dirname(__FILE__), 'plugin', 'data')

module MongoTestHelper
  def self.cleanup_mongod_env
    Process.kill "TERM", @@pid
    Process.waitpid @@pid
    system("rm -rf #{MONGO_DB_PATH}")
  end

  def self.setup_mongod
    system("rm -rf #{MONGO_DB_PATH}")
    system("mkdir -p #{MONGO_DB_PATH}")

    @@mongod_port = unused_port
    @@pid = spawn('mongod', "--port=#{@@mongod_port}", "--replSet=rs0",
                  "--dbpath=#{MONGO_DB_PATH}")
    sleep 3
    Open3.popen3("mongo --port #{@@mongod_port}"){|stdin, _stdout, _stderr, _wait_thr|
      stdin.puts "rs.initiate()"
      stdin.close
      p _stdout.read
    }
    sleep 3
  end

  def self.teardown_mongod
    cleanup_mongod_env
  end

  def self.mongod_port
    @@mongod_port
  end
end
