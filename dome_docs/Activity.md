> ## Documentation Index
> Fetch the complete documentation index at: https://docs.domeapi.io/llms.txt
> Use this file to discover all available pages before exploring further.

# Activity

> Fetches activity data with optional filtering by user, market, condition, and time range. Returns trading activity including `MERGES`, `SPLITS`, and `REDEEMS`. Supports efficient cursor-based pagination.



## OpenAPI

````yaml api-reference/openapi.json get /polymarket/activity
openapi: 3.0.3
info:
  title: Dome API
  description: APIs for prediction markets.
  version: 0.0.1
servers:
  - url: https://api.domeapi.io/v1
security: []
paths:
  /polymarket/activity:
    get:
      summary: Get Activity
      description: >-
        Fetches activity data with optional filtering by user, market,
        condition, and time range. Returns trading activity including `MERGES`,
        `SPLITS`, and `REDEEMS`. Supports efficient cursor-based pagination for
        large datasets.
      operationId: getActivity
      parameters:
        - name: user
          in: query
          required: false
          description: >-
            User wallet address to fetch activity for. If not provided, returns
            activity for all users.
          schema:
            type: string
            pattern: ^0x[0-9a-fA-F]{40}$
            example: '0x7c3db723f1d4d8cb9c550095203b686cb11e5c6b'
        - name: start_time
          in: query
          required: false
          description: Filter activity from this Unix timestamp in seconds (inclusive)
          schema:
            type: integer
            example: 1640995200
        - name: end_time
          in: query
          required: false
          description: Filter activity until this Unix timestamp in seconds (inclusive)
          schema:
            type: integer
            example: 1672531200
        - name: market_slug
          in: query
          required: false
          description: Filter activity by market slug
          schema:
            type: string
            example: bitcoin-up-or-down-july-25-8pm-et
        - name: condition_id
          in: query
          required: false
          description: Filter activity by condition ID
          schema:
            type: string
            example: '0x4567b275e6b667a6217f5cb4f06a797d3a1eaf1d0281fb5bc8c75e2046ae7e57'
        - name: limit
          in: query
          required: false
          description: Number of activities to return (1-1000)
          schema:
            type: integer
            minimum: 1
            maximum: 1000
            default: 100
            example: 50
        - name: pagination_key
          in: query
          required: false
          description: >-
            Base64-encoded cursor for efficient pagination. Returned in the
            previous response's pagination object.
          schema:
            type: string
            example: >-
              eyJibG9ja190aW1lc3RhbXAiOiIyMDI1LTAxLTE5VDEyOjAwOjAwLjAwMFoiLCJzaWRlIjoiU1BMSVQiLCJ0eF9oYXNoIjoiMHgxMjM0NTY3ODkwYWJjZGVmIiwibG9nX2luZGV4IjoxMCwidG90YWwiOjE5ODkwMTc3Nn0=
      responses:
        '200':
          description: Activity response with pagination
          content:
            application/json:
              schema:
                type: object
                properties:
                  activities:
                    type: array
                    items:
                      type: object
                      properties:
                        token_id:
                          type: string
                          example: ''
                        side:
                          type: string
                          enum:
                            - MERGE
                            - SPLIT
                            - REDEEM
                          example: REDEEM
                        market_slug:
                          type: string
                          example: will-the-doj-charge-boeing
                        condition_id:
                          type: string
                          example: >-
                            0x92e4b1b8e0621fab0537486e7d527322569d7a8fd394b3098ff4bb1d6e1c0bbd
                        shares:
                          type: number
                          example: 187722726
                          description: Raw number of shares (from the blockchain)
                        shares_normalized:
                          type: number
                          example: 187.722726
                          description: Number of shares normalized (raw divided by 1000000)
                        price:
                          type: number
                          example: 1
                        block_number:
                          type: integer
                          description: Block number where the activity occurred
                          example: 123456789
                        log_index:
                          type: integer
                          description: Log index of the activity event in the block
                          example: 42
                        tx_hash:
                          type: string
                          example: >-
                            0x028baff23a90c10728606781d15077098ee93c991ea204aa52a0bd2869187574
                        title:
                          type: string
                          example: Will the DOJ charge Boeing?
                        timestamp:
                          type: integer
                          description: Unix timestamp in seconds when the activity occurred
                          example: 1721263049
                        order_hash:
                          type: string
                          example: ''
                        user:
                          type: string
                          description: User wallet address
                          example: '0xfd9c3e7f8c56eb4186372f343c873cce154b3873'
                  pagination:
                    type: object
                    properties:
                      limit:
                        type: integer
                        example: 50
                      count:
                        type: integer
                        description: Total number of activities matching the filters
                        example: 1250
                      has_more:
                        type: boolean
                        description: Whether there are more activities available
                        example: true
                      pagination_key:
                        type: string
                        description: >-
                          Base64-encoded cursor for the next page. Only present
                          when there are more results.
                        example: >-
                          eyJibG9ja190aW1lc3RhbXAiOiIyMDI1LTAxLTE5VDEyOjAwOjAwLjAwMFoiLCJzaWRlIjoiU1BMSVQiLCJ0eF9oYXNoIjoiMHgxMjM0NTY3ODkwYWJjZGVmIiwibG9nX2luZGV4IjoxMCwidG90YWwiOjE5ODkwMTc3Nn0=
        '400':
          description: Bad Request - Invalid parameters or validation errors
          content:
            application/json:
              schema:
                type: object
                properties:
                  error:
                    type: string
                    example: Missing required parameter
                  message:
                    type: string
                    example: user parameter is required
              examples:
                invalid_start_time:
                  summary: Invalid start_time parameter
                  value:
                    error: Invalid start_time parameter
                    message: start_time must be a valid Unix timestamp
                invalid_end_time:
                  summary: Invalid end_time parameter
                  value:
                    error: Invalid end_time parameter
                    message: end_time must be a valid Unix timestamp
                invalid_time_range:
                  summary: Invalid time range
                  value:
                    error: Invalid time range
                    message: start_time must be less than end_time
                invalid_limit:
                  summary: Invalid limit
                  value:
                    error: Invalid limit parameter
                    message: limit must be a number between 1 and 1000
                invalid_pagination_key:
                  summary: Invalid pagination key
                  value:
                    error: Invalid pagination key
                    message: pagination_key is invalid or corrupted
                invalid_filter_combination:
                  summary: Invalid filter combination
                  value:
                    error: Invalid filter combination
                    message: Only one of market_slug or condition_id can be provided
        '500':
          description: Internal Server Error
          content:
            application/json:
              schema:
                type: object
                properties:
                  error:
                    type: string
                    example: Internal Server Error
                  message:
                    type: string
                    example: Failed to fetch activity data

````

Built with [Mintlify](https://mintlify.com).