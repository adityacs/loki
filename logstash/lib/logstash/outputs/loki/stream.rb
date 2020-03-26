module LogStash
    module Outputs
        class Loki 
            class Stream
                attr_reader :labels, :entries
                def add(labels, entries)
                    @labels   = labels
                    @entries = entries
                end 
            end 
        end 
    end 
end 