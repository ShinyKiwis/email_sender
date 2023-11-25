class PushQueueMessageJob < ApplicationJob
  sqs_event(:generate_queue, queue_properties: {
    queue_name: 'email'
  })
  def consumer
    puts "dig #{JSON.dump(event)}"
  end
end