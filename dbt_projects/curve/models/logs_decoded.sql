{{ config(materialized='table') }}

/*
Ethereum event logs typically contain:
- topic0: Event signature/hash (keccak256 of the event name and parameter types)
- topic1-topic4: Indexed parameters from the event
  - Up to 3 indexed parameters are common
  - Maximum of 4 indexed parameters possible
Note: Non-indexed parameters are stored in the data field
*/

SELECT 
    -- Keep original topics array for reference
    topics as topics_raw,
    
    -- Extract topics into separate columns
    json_extract_string(topics, '$[0]') as topic0,  -- Event signature
    CASE 
        WHEN json_array_length(topics) >= 2 THEN json_extract_string(topics, '$[1]')
        ELSE NULL 
    END as topic1,
    CASE 
        WHEN json_array_length(topics) >= 3 THEN json_extract_string(topics, '$[2]')
        ELSE NULL 
    END as topic2,
    CASE 
        WHEN json_array_length(topics) >= 4 THEN json_extract_string(topics, '$[3]')
        ELSE NULL 
    END as topic3,
    CASE 
        WHEN json_array_length(topics) >= 5 THEN json_extract_string(topics, '$[4]')
        ELSE NULL 
    END as topic4,

    -- Keep all other columns
    address,
    data,
    block_number,
    block_hash,
    time_stamp,
    gas_price,
    gas_used,
    log_index,
    transaction_hash,
    transaction_index,
FROM curve.log_raw
