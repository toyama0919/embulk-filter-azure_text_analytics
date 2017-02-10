require "json"
require "net/http"
require "uri"
require "pp"

module Embulk
  module Filter

    class AzureTextAnalyticsTopics < FilterPlugin
      Plugin.register_filter("azure_text_analytics_topics", self)
      ENDPOINT_PREFIX = "https://westus.api.cognitive.microsoft.com/text/analytics/v2.0"

      def self.transaction(config, in_schema, &control)
        task = {
          "language" => config.param("language", :string, default: nil),
          "out_key_name" => config.param("out_key_name", :string),
          "key_name" => config.param("key_name", :string),
          "body_params" => config.param("body_params", :hash, default: {}),
          "params" => config.param("params", :hash, default: {}),
          "subscription_key" => config.param("subscription_key", :string),
          "stop_words" => config.param("stop_words", :array, default: nil),
          "stop_phrases" => config.param("stop_phrases", :array, default: nil),
        }

        out_columns = [
          Column.new(nil, task["out_key_name"], :json)
        ]

        yield(task, out_columns)
      end

      def init
        @subscription_key = task['subscription_key']
        @body_params = task['body_params']
        @key_name = task['key_name']
        @language = task['language']
        @out_key_name = task['out_key_name']
        @stop_words = task['stop_words']
        @stop_phrases = task['stop_phrases']

        uri_string = "#{ENDPOINT_PREFIX}/topics"
        @uri = URI.parse(uri_string)
        @uri.query = URI.encode_www_form(task['params'])
        @http = Net::HTTP.new(@uri.host, @uri.port)
        @http.use_ssl = true

        @request = Net::HTTP::Post.new(@uri.request_uri)
        @request['Content-Type'] = 'application/json'
        @request['Ocp-Apim-Subscription-Key'] = @subscription_key

        @records = []
      end

      def close
      end

      def add(page)
        page.each do |record|
          @records << Hash[in_schema.names.zip(record)]
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
        documents = []
        records.each do |record|
          document = {}
          document_id = SecureRandom.uuid
          document["language"] = @language if @language
          document["id"] = document_id
          document["text"] = record[@key_name]
          documents << document
        end

        request_body_hash = @body_params.merge({ documents: documents })
        request_body_hash[:stopWords] = @stop_words if @stop_words
        request_body_hash[:stopPhrases] = @stop_phrases if @stop_phrases

        @request.body = request_body_hash.to_json
        Embulk.logger.debug "request body => #{@request.body}"
        response_hash = @http.start do |h|
          proc_http(h)
        end

        topics = response_hash["operationProcessingResult"]["topics"]

        topics.each do |data|
          page_builder.add([data])
        end
      end

      def proc_http(h)
        response = h.request(@request)
        operation_location = response['operation-location']

        if operation_location
          Embulk.logger.info "operation_location => #{operation_location}"
          topics_request = Net::HTTP::Get.new(operation_location)
          topics_request['Ocp-Apim-Subscription-Key'] = @subscription_key
          loop do
            topics_response = h.request(topics_request)
            topics_response_body = topics_response.body
            topics_response_hash = JSON.parse(topics_response_body)
            status = topics_response_hash['status']
            Embulk.logger.info "status => #{status}"
            if status == 'Succeeded'
              Embulk.logger.debug "topics_response_hash => #{topics_response_hash}"
              return topics_response_hash
            end
            if status == 'Failed'
              raise "topics_response_hash => #{topics_response_hash}"
            end
            sleep 60
          end
        end

        JSON.parse(response.body)
      end
    end
  end
end
