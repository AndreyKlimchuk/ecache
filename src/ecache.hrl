-record(datum, {key :: term(),
                mgr, % ???
                data :: term(),
                started :: integer(),
                reaper :: undefined|pid(),
                last_active :: integer(),
                ttl = unlimited :: unlimited|non_neg_integer(),
                type = mru :: mru|actual_time,
                remaining_ttl = unlimited :: unlimited|non_neg_integer()}).
