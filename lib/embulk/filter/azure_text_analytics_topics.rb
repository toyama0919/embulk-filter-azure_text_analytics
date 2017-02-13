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
          "operation_id" => config.param("operation_id", :string, default: nil),
          "stop_words" => config.param("stop_words", :array, default: nil),
          "stop_phrases" => config.param("stop_phrases", :array, default: nil),
          "id_format" => config.param("id_format", :string, default: nil),
          "id_keys" => config.param("id_keys", :array, default: []),
        }

        add_columns = [
          Column.new(nil, task['out_key_name'], :json)
        ]

        out_columns = in_schema + add_columns

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
        @id_format = task['id_format']
        @id_keys = task['id_keys']
        @operation_location = if task['operation_id']
          "https://westus.api.cognitive.microsoft.com/text/analytics/v2.0/operations/" + task['operation_id']
        end

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
          document_id = if (@id_format && @id_keys)
            generate_id(@id_format, record, @id_keys)
          else
            SecureRandom.uuid
          end
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
          @operation_location ? proc_topics(h, @operation_location) : proc_http(h)
        end
        topics = response_hash["operationProcessingResult"]["topics"]
        topics_assignments = response_hash["operationProcessingResult"]["topicAssignments"]

        topics_assignments_merged = topics_assignments.map do |topics_assignment|
          selected_topics = topics.select { |topic| topics_assignment['topicId'] == topic['id'] }
          result = topics_assignment.merge(selected_topics.first)
          result.delete('id')
          result
        end

        documents.each_with_index do |document, i|
          record = records[i]
          selected_topics_assignments = topics_assignments_merged.select{ |topics_assignment|
            topics_assignment.delete('topicId')
            topics_assignment['documentId'] == document['id']
          }
          page_builder.add(record.values + [selected_topics_assignments])
        end
      end

      def generate_id(template, record, id_keys)
        template % id_keys.map { |key| record[key] }
      end

      def proc_http(h)
        response = h.request(@request)
        operation_location = response['operation-location']

        if operation_location
          return proc_topics(h, operation_location)
        end

        JSON.parse(response.body)
      end

      def proc_topics(h, operation_location)
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
    end
  end
end
