require "json"
require "net/http"
require "uri"

module Embulk
  module Filter

    class AzureTextAnalytics < FilterPlugin
      Plugin.register_filter("azure_text_analytics", self)
      ENDPOINT_PREFIX = "https://westus.api.cognitive.microsoft.com/text/analytics/v2.0"

      def self.transaction(config, in_schema, &control)
        task = {
          "api_type" => config.param("api_type", :string),
          "language" => config.param("language", :string, default: nil),
          "out_key_name" => config.param("out_key_name", :string),
          "key_name" => config.param("key_name", :string),
          "body_params" => config.param("body_params", :hash, default: {}),
          "params" => config.param("params", :hash, default: {}),
          "delay" => config.param("delay", :integer, default: 0),
          "per_request" => config.param("per_request", :integer, default: 1),
          "bulk_size" => config.param("bulk_size", :integer, default: 100),
          "subscription_key" => config.param("subscription_key", :string),
        }

        if task['api_type'] == 'topics'
          raise ConfigError.new "Not support type topics API. use azure_text_analytics_topics."
        end

        add_columns = [
          Column.new(nil, task["out_key_name"], :json)
        ]

        out_columns = in_schema + add_columns

        yield(task, out_columns)
      end

      def init
        uri_string = "#{ENDPOINT_PREFIX}/#{task['api_type']}"
        @uri = URI.parse(uri_string)
        @uri.query = URI.encode_www_form(task['params'])
        @http = Net::HTTP.new(@uri.host, @uri.port)
        @http.use_ssl = true
        @request = Net::HTTP::Post.new(@uri.request_uri)
        @request['Content-Type'] = 'application/json'
        @request['Ocp-Apim-Subscription-Key'] = task['subscription_key']

        @body_params = task['body_params']
        @per_request = task['per_request']
        @delay = task['delay']
        @key_name = task['key_name']
        @language = task['language']
        @out_key_name = task['out_key_name']
        @bulk_size = task['bulk_size']
        @records = []
      end

      def close
      end

      def add(page)
        page.each do |record|
          @records << Hash[in_schema.names.zip(record)]
          if @records.size == @bulk_size
            proc_records(@records)
            @records = []
          end
        end
      end

      def finish
        if @records.size > 0
          proc_records(@records)
        end
        page_builder.finish
      end

      private
      def proc_records(records)
        documents = {}
        records.each do |record|
          document = {}
          uuid = SecureRandom.uuid
          document["language"] = @language if @language
          document["id"] = uuid
          document["text"] = record[@key_name]
          documents[uuid] = document
        end

        @request.body = @body_params.merge({ documents: documents.values }).to_json
        Embulk.logger.debug "request body => #{@request.body}"
        response_hash = @http.start do |h|
          response = h.request(@request)
          JSON.parse(response.body)
        end

        if response_hash.key?('innerError')
          Embulk.logger.error "response body => #{response_hash}"
        end

        records.each_with_index do |record, i|
          record[@out_key_name] = if response_hash.key?('innerError')
            response_hash
          else
            response_hash['documents'][i]
          end
          page_builder.add(record.values)
        end
        sleep @delay
      end
    end
  end
end
