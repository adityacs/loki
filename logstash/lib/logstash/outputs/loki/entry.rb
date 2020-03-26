module LogStash
    module Outputs
        class Loki 
            class Entry
                attr_reader :tenant_id, :labels, :entry
                def initialize(tenant_id, labels, entry)
                    @tenant_id  = tenant_id
                    @labels   = labels
                    @entry = entry
                end 
            end 
        end 
    end 
end 