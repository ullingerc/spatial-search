-- Increase memory allowance, requires restart of the server
ALTER SYSTEM SET temp_buffers = '20GB';  -- default: 8MB
ALTER SYSTEM SET shared_buffers = '5GB';  -- default: 128MB
ALTER SYSTEM SET work_mem = '250MB';  -- default: 4MB
