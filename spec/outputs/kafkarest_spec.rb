require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/kafkarest"
require "logstash/codecs/plain"
require "thread"
require "sinatra"
require_relative "../supports/compressed_requests"

PORT = rand(65535-1024) + 1025

class LogStash::Outputs::KafkaRest
  attr_writer :agent
  attr_reader :request_tokens
end

# note that Sinatra startup and shutdown messages are directly logged to stderr so
# it is not really possible to disable them without reopening stderr which is not advisable.
#
# == Sinatra (v1.4.6) has taken the stage on 51572 for development with backup from WEBrick
# == Sinatra has ended his set (crowd applauds)
#
class TestApp < Sinatra::Base
  # on the fly uncompress gzip content
  use CompressedRequests

  # disable WEBrick logging
  def self.server_settings
    { :AccessLog => [], :Logger => WEBrick::BasicLog::new(nil, WEBrick::BasicLog::FATAL) }
  end

  def self.multiroute(methods, path, &block)
    methods.each do |method|
      method.to_sym
      self.send method, path, &block
    end
  end

  def self.last_request=(request)
    @last_request = request
  end

  def self.last_request
    @last_request
  end

  def self.retry_fail_count=(count)
    @retry_fail_count = count
  end

  def self.retry_fail_count()
    @retry_fail_count || 2
  end

  multiroute(%w(get post put patch delete), "/good") do
    self.class.last_request = request
    [200, "YUP"]
  end

  multiroute(%w(get post put patch delete), "/bad") do
    self.class.last_request = request
    [400, "YUP"]
  end

  multiroute(%w(get post put patch delete), "/retry") do
    self.class.last_request = request

    if self.class.retry_fail_count > 0
      self.class.retry_fail_count -= 1
      [429, "Will succeed in #{self.class.retry_fail_count}"]
    else
      [200, "Done Retrying"]
    end
  end
end

RSpec.configure do |config|
  #http://stackoverflow.com/questions/6557079/start-and-call-ruby-http-server-in-the-same-script
  def sinatra_run_wait(app, opts)
    queue = Queue.new

    t = java.lang.Thread.new(
      proc do
        begin
          app.run!(opts) do |server|
            queue.push("started")
          end
        rescue => e
          puts "Error in webserver thread #{e}"
          # ignore
        end
      end
    )
    t.daemon = true
    t.start
    queue.pop # blocks until the run! callback runs
  end

  config.before(:suite) do
    sinatra_run_wait(TestApp, :port => PORT, :server => 'webrick')
    puts "Test webserver on port #{PORT}"
  end
end

describe LogStash::Outputs::KafkaRest do
  # Wait for the async request to finish in this spinlock
  # Requires pool_max to be 1

  let(:port) { PORT }
  let(:event) {
    LogStash::Event.new({"message" => "hi"})
  }
  let(:url) { "http://localhost:#{port}/good" }
  let(:method) { "post" }

  shared_examples("verb behavior") do |method|
    let(:verb_behavior_config) { {"url" => url, "pool_max" => 1} }
    subject { LogStash::Outputs::KafkaRest.new(verb_behavior_config) }

    let(:expected_method) { method.clone.to_sym }
    let(:client) { subject.client }

    before do
      subject.register
      allow(client).to receive(:send).
                         with(expected_method, url, anything).
                         and_call_original
      allow(subject).to receive(:log_failure).with(any_args)
      allow(subject).to receive(:log_retryable_response).with(any_args)
    end

    context 'sending no events' do
      it 'should not block the pipeline' do
        subject.multi_receive([])
      end
    end

    context "performing a post" do
      describe "invoking the request" do
        before do
          subject.multi_receive([event])
        end

        it "should execute the request" do
          expect(client).to have_received(:send).
                              with(expected_method, url, anything)
        end
      end

      context "with passing requests" do
        before do
          subject.multi_receive([event])
        end

        it "should not log a failure" do
          expect(subject).not_to have_received(:log_failure).with(any_args)
        end
      end

      context "with failing requests" do
        let(:url) { "http://localhost:#{port}/bad"}

        before do
          subject.multi_receive([event])
        end

        it "should log a failure" do
          expect(subject).to have_received(:log_failure).with(any_args)
        end
      end

      context "with ignorable failing requests" do
        let(:url) { "http://localhost:#{port}/bad"}
        let(:verb_behavior_config) { super().merge("ignorable_codes" => [400]) }

        before do
          subject.multi_receive([event])
        end

        it "should log a failure" do
          expect(subject).not_to have_received(:log_failure).with(any_args)
        end
      end

      context "with retryable failing requests" do
        let(:url) { "http://localhost:#{port}/retry"}

        before do
          TestApp.retry_fail_count=2
          allow(subject).to receive(:send_event).and_call_original
          subject.multi_receive([event])
        end

        it "should log a retryable response 2 times" do
          expect(subject).to have_received(:log_retryable_response).with(any_args).twice
        end

        it "should make three total requests" do
          expect(subject).to have_received(:send_event).exactly(3).times
        end
      end
    end

    context "on exception" do
      before :each do
        allow(subject.client).to receive(:send).and_raise RuntimeError
        subject.multi_receive([event])
      end

      it "should not log headers" do
        expect(subject).to have_received(:log_failure).with(anything, hash_not_including(:headers))
      end

      it "should not log the body" do
        expect(subject).to have_received(:log_failure).with(anything, hash_not_including(:body))
      end

      context "with debug log level" do
        before :all do
          @current_log_level = LogStash::Logging::Logger.get_logging_context.get_root_logger.get_level.to_s.downcase
          LogStash::Logging::Logger.configure_logging "debug"
        end
        after :all do
          LogStash::Logging::Logger.configure_logging @current_log_level
        end

        it "should log a failure" do
          expect(subject).to have_received(:log_failure).with(any_args)
        end

        it "should not log headers" do
          expect(subject).to have_received(:log_failure).with(anything, hash_including(:headers))
        end

        it "should not log the body" do
          expect(subject).to have_received(:log_failure).with(anything, hash_including(:body))
        end
      end
    end
  end

  shared_examples("a received event") do
    before do
      TestApp.last_request = nil
    end

    let(:events) { [event] }

    describe "with a good code" do
      before do
        subject.multi_receive(events)
      end

      let(:last_request) { TestApp.last_request }
      let(:body) { last_request.body.read }
      let(:content_type) { last_request.env["CONTENT_TYPE"] }

      it "should receive the request" do
        expect(last_request).to be_truthy
      end

      it "should receive the event as a hash" do
        expect(body).to eql(expected_body)
      end

      it "should have the correct content type" do
        expect(content_type).to eql(expected_content_type)
      end
    end

    describe "a retryable code" do
      let(:url) { "http://localhost:#{port}/retry" }

      before do
        TestApp.retry_fail_count=2
        allow(subject).to receive(:send_event).and_call_original
        allow(subject).to receive(:log_retryable_response)
        subject.multi_receive(events)
      end

      it "should retry" do
        expect(subject).to have_received(:log_retryable_response).with(any_args).twice
      end
    end
  end

  shared_examples "integration tests" do
    let(:base_config) { {} }
    let(:url) { "http://localhost:#{port}/good" }
    let(:event) {
      LogStash::Event.new("foo" => "bar", "baz" => "bot", "user" => "McBest")
    }

    subject { LogStash::Outputs::KafkaRest.new(config) }

    before do
      subject.register
    end

    describe "sending batch with one event" do
      let(:config) {
        base_config.merge({"url" => url, "pool_max" => 1})
      }
      let(:expected_body) { LogStash::Json.dump({:records => [{:value => event}]}) }
      let(:expected_content_type) { "application/vnd.kafka.json.v2+json" }

      include_examples("a received event")
    end

    describe "sending batch with two events" do
      let(:config) do
        base_config.merge({"url" => url})
      end

      let(:expected_body) { ::LogStash::Json.dump({:records => events.map { |event| {:value => event} }}) }
      let(:events) { [::LogStash::Event.new("a" => 1), ::LogStash::Event.new("b" => 2)]}
      let(:expected_content_type) { "application/vnd.kafka.json.v2+json" }

      include_examples("a received event")

    end

    describe "sending batch with one event and value schema id" do
      let(:config) {
        base_config.merge({"url" => url, "pool_max" => 1, "value_schema_id" => 12345})
      }
      let(:expected_body) { LogStash::Json.dump({:records => [{:value => event}], :value_schema_id => 12345}) }
      let(:expected_content_type) { "application/vnd.kafka.jsonschema.v2+json" }

      include_examples("a received event")
    end

    describe "sending no batch with one event" do
      let(:config) {
        base_config.merge({"url" => url, "pool_max" => 1, "batch_events" => false})
      }
      let(:expected_body) { LogStash::Json.dump({:records => [{:value => event}]}) }
      let(:expected_content_type) { "application/vnd.kafka.json.v2+json" }

      include_examples("a received event")
    end

    describe "sending no batch with two events" do
      let(:config) do
        base_config.merge({"url" => url, "batch_events" => false})
      end

      let(:expected_body) { ::LogStash::Json.dump({:records => events.map { |event| {:value => event} }}) }
      let(:events) { [::LogStash::Event.new("a" => 1), ::LogStash::Event.new("b" => 2)]}
      let(:expected_content_type) { "application/vnd.kafka.json.v2+json" }

      include_examples("a received event")

    end

    describe "sending a mapped event" do
      let(:config) {
        base_config.merge({"url" => url, "pool_max" => 1, "mapping" => {"blah" => "X %{foo}"} })
      }
      let(:expected_body) { LogStash::Json.dump({:records => [{:value => {:blah => "X #{event.get("foo")}"}}]}) }
      let(:expected_content_type) { "application/vnd.kafka.json.v2+json" }

      include_examples("a received event")
    end

    describe "sending a mapped, nested event" do
      let(:config) {
        base_config.merge({
          "url" => url,
          "pool_max" => 1,
          "mapping" => {
            "host" => "X %{foo}",
            "event" => {
              "user" => "Y %{user}"
            },
            "arrayevent" => [{
              "user" => "Z %{user}"
            }]
          }
        })
      }
      let(:expected_body) {
        LogStash::Json.dump({
          :records => [{
            :value => {
              :host => "X #{event.get("foo")}",
              :event => {
                :user => "Y #{event.get("user")}"
              },
              :arrayevent => [{
                "user" => "Z #{event.get("user")}"
              }]
            }
          }]
        })
      }
      let(:expected_content_type) { "application/vnd.kafka.json.v2+json" }

      include_examples("a received event")
    end
  end

  describe "integration test without gzip compression" do
    include_examples("integration tests")
  end

  describe "integration test with gzip compression" do
    include_examples("integration tests") do
      let(:base_config) { { "http_compression" => true } }
    end
  end
end
