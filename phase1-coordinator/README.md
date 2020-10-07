# phase1-coordinator

## Testing

### Logging

To enable `tracing` on a specific test, add `test_logger()` at the start of the test function.

### Serial Execution

By default, all test functions execute serially to minimize risk of writing over test storage.