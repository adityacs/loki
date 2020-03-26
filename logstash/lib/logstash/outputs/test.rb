#require '/Library/Ruby/Gems/2.3.0/gems/concurrent-ruby-1.1.6/lib/concurrent-ruby/concurrent'
require 'concurrent-edge'
Channel = Concurrent::Channel

## Go by Example: Channel Buffering
# https://gobyexample.com/channel-buffering
puts "Hi"
@batches = {}
if !@batches.has_key?("fake")
  puts "hi2"
end
# messages = Channel.new # buffered
# tick = Concurrent::Channel.tick(1)  # alias for `ticker`
# Channel.go do
#   messages << 'buffered'
# end

# puts "Hi"
# Thread.new {
#   loop do
#     Concurrent::Channel.select do |s|
#       s.take(messages) { |message| puts message }
#       s.default {
#         puts "hi"
#         sleep(5)
#       }
#     end
#   end
# }



#  puts messages.take
#  puts messages.take

# def worker(done_channel, quit)
#   loop do
#     Concurrent::Channel.select do |s|
#       s.take(done_channel) { |message| 
#         puts "Inside take message"
#         puts message 
#         if message != "test"
#           next
#         end 
#         puts "After Inside take message"
#       }
#       s.take(quit) {
#         puts "hi"
#         sleep(5)
#         quit << "quit"
#       }
#       s.default {
#         done_channel << "hibye"
#       }
#     end
#   end
# end

# done = Channel.new(capacity: 1) # buffered
# quit = Channel.new(capacity: 1)
# Channel.go{ worker(done, quit) }

#puts quit.take
