class PushQueueMessageJob < ApplicationJob
  class_timeout 30
  sqs_event(:generate_queue, queue_properties: {
    queue_name: 'email'
  })
end