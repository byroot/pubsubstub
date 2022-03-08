require 'net/http'

module HTTPHelpers
  def async_get(uri, headers = {}, retries: 10, &block)
    uri = URI(uri.to_s)
    queue = Queue.new
    Thread.start do
      begin
        Net::HTTP.start(uri.host, uri.port, open_timeout: 10) do |http|
          request = Net::HTTP::Get.new uri.request_uri
          headers.each do |name, value|
            request.add_field(name, value)
          end

          http.request request do |response|
            response.read_body do |chunk|
              queue.push(chunk) unless chunk.empty?
            end
          end
        end
      rescue Errno::ECONNRESET, Errno::EPIPE, Errno::EINVAL, Errno::ECONNREFUSED => e
        puts
        p e
        if retries > 0
          retries -= 1
          sleep 0.5
          retry
        else
          raise
        end
      end
    end

    queue
  end

  ROOT_PATH = File.join(__dir__, '../..')
  LOG_DIR = File.join(ROOT_PATH, 'tmp')
  LOG_PATH = File.join(ROOT_PATH, 'tmp/puma.log')
  def with_background_server
    Dir.mkdir(LOG_DIR) unless Dir.exist?(LOG_DIR)
    server_pid = Process.spawn('bin/server', chdir: ROOT_PATH)#, out: LOG_PATH, err: LOG_PATH)
    begin
      yield
    ensure
      Process.kill('TERM', server_pid)
      Process.wait(server_pid)
    end
  end
end
