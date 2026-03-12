> ## Documentation Index
> Fetch the complete documentation index at: https://docs.domeapi.io/llms.txt
> Use this file to discover all available pages before exploring further.

# Positions

> Fetches all Polymarket positions for a proxy wallet address. Returns positions with balance >= 10,000 shares (0.01 normalized) with market info.



## OpenAPI

````yaml api-reference/openapi.json get /polymarket/positions/wallet/{wallet_address}
openapi: 3.0.3
info:
  title: Dome API
  description: APIs for prediction markets.
  version: 0.0.1
servers:
  - url: https://api.domeapi.io/v1
security: []
paths:
  /polymarket/positions/wallet/{wallet_address}:
    parameters:
      - name: wallet_address
        in: path
        required: true
        description: The Proxy wallet address to fetch positions for
        schema:
          type: string
          pattern: ^0x[0-9a-fA-F]{40}$
          example: '0x1234567890abcdef1234567890abcdef12345678'
    get:
      summary: Get Positions
      description: >-
        Fetches all Polymarket positions for a proxy wallet address. Returns
        positions with balance >= 10,000 shares (0.01 normalized) with market
        info.
      operationId: getPositions
      parameters:
        - name: limit
          in: query
          required: false
          description: >-
            Maximum number of positions to return per page. Defaults to 100,
            maximum 100.
          schema:
            type: integer
            minimum: 1
            maximum: 100
            default: 100
            example: 100
        - name: pagination_key
          in: query
          required: false
          description: >-
            Pagination key returned from previous request to fetch next page of
            results
          schema:
            type: string
            example: NjhlMGM4MDAtZDkwNi00YjMyLWEzOWMtYTRlMTk5MzRkNWVl
      responses:
        '200':
          description: Positions response
          content:
            application/json:
              schema:
                type: object
                properties:
                  wallet_address:
                    type: string
                    description: The wallet address (normalized to lowercase)
                    example: '0x1234567890abcdef1234567890abcdef12345678'
                  positions:
                    type: array
                    description: Array of position objects
                    items:
                      type: object
                      properties:
                        wallet:
                          type: string
                          description: The wallet address
                          example: '0x1234567890abcdef1234567890abcdef12345678'
                        token_id:
                          type: string
                          description: The Polymarket token ID for this position
                          example: >-
                            19701256321759583954581192053894521654935987478209343000964756587964612528044
                        condition_id:
                          type: string
                          description: The condition ID for the market
                          example: 0x1234abcd...
                        title:
                          type: string
                          description: Market title
                          example: Will Bitcoin reach $100k by end of 2025?
                        shares:
                          type: integer
                          description: Number of shares (raw, not normalized)
                          example: 50000000
                        shares_normalized:
                          type: number
                          description: Number of shares normalized (divided by 1,000,000)
                          example: 50
                        redeemable:
                          type: boolean
                          description: >-
                            Whether this position can be redeemed (market
                            completed and is closed)
                          example: false
                        market_slug:
                          type: string
                          description: The market slug
                          example: will-bitcoin-reach-100k-by-end-of-2025
                        event_slug:
                          type: string
                          description: The event slug
                          example: bitcoin-price-predictions
                        image:
                          type: string
                          description: URL to market image
                          example: https://polymarket.com/images/...
                        label:
                          type: string
                          description: >-
                            The outcome label for this token (e.g., 'Yes' or
                            'No')
                          example: 'Yes'
                        winning_outcome:
                          type: object
                          nullable: true
                          description: >-
                            Information about the winning outcome if market is
                            closed, null if market is still open or no outcome
                            determined
                          properties:
                            id:
                              type: string
                              description: Token ID of the winning outcome
                              example: >-
                                19701256321759583954581192053894521654935987478209343000964756587964612528044
                            label:
                              type: string
                              description: Label of the winning outcome
                              example: 'Yes'
                        start_time:
                          type: integer
                          description: Market start timestamp (Unix seconds)
                          example: 1640995200
                        end_time:
                          type: integer
                          description: Market end timestamp (Unix seconds)
                          example: 1672531200
                        completed_time:
                          type: integer
                          nullable: true
                          description: >-
                            Market completion timestamp (Unix seconds),
                            currently always null
                          example: null
                        close_time:
                          type: integer
                          nullable: true
                          description: >-
                            Market close timestamp (Unix seconds) if market is
                            closed, null if still open
                          example: 1672531200
                        game_start_time:
                          type: string
                          nullable: true
                          description: >-
                            Game start time for sports markets, null for
                            non-sports markets
                          example: '2025-01-15T19:00:00Z'
                        market_status:
                          type: string
                          enum:
                            - open
                            - closed
                          description: Whether the market is open or closed
                          example: open
                        negativeRisk:
                          type: boolean
                          description: Whether this market has negative risk
                          example: false
                      required:
                        - wallet
                        - token_id
                        - condition_id
                        - title
                        - shares
                        - shares_normalized
                        - redeemable
                        - market_slug
                        - event_slug
                        - image
                        - label
                        - winning_outcome
                        - start_time
                        - end_time
                        - completed_time
                        - close_time
                        - game_start_time
                        - market_status
                        - negativeRisk
                  pagination:
                    type: object
                    description: Pagination information
                    properties:
                      has_more:
                        type: boolean
                        description: Whether there are more results available
                        example: false
                      limit:
                        type: integer
                        description: The limit used for this request
                        example: 100
                      pagination_key:
                        type: string
                        nullable: true
                        description: >-
                          Pagination key to use for fetching next page, null if
                          no more pages
                        example: null
                    required:
                      - has_more
                      - limit
                      - pagination_key
                required:
                  - wallet_address
                  - positions
                  - pagination
              examples:
                with_positions:
                  summary: Wallet with positions
                  description: Response when wallet has active positions
                  value:
                    wallet_address: '0x1234567890abcdef1234567890abcdef12345678'
                    positions:
                      - wallet: '0x1234567890abcdef1234567890abcdef12345678'
                        token_id: >-
                          19701256321759583954581192053894521654935987478209343000964756587964612528044
                        condition_id: 0xabcd1234...
                        title: Will Bitcoin reach $100k by end of 2025?
                        shares: 50000000
                        shares_normalized: 50
                        redeemable: false
                        market_slug: will-bitcoin-reach-100k-by-end-of-2025
                        event_slug: bitcoin-price-predictions
                        image: https://polymarket.com/images/...
                        label: 'Yes'
                        winning_outcome: null
                        start_time: 1640995200
                        end_time: 1672531200
                        completed_time: null
                        close_time: null
                        game_start_time: null
                        market_status: open
                        negativeRisk: false
                    pagination:
                      has_more: false
                      limit: 100
                      pagination_key: null
                empty_positions:
                  summary: Wallet with no positions
                  description: Response when wallet has no positions >= 10,000 shares
                  value:
                    wallet_address: '0x1234567890abcdef1234567890abcdef12345678'
                    positions: []
                    pagination:
                      has_more: false
                      limit: 100
                      pagination_key: null
        '400':
          description: Bad Request - Invalid parameters
          content:
            application/json:
              schema:
                type: object
                properties:
                  error:
                    type: string
                    example: Invalid wallet address
                  message:
                    type: string
                    example: >-
                      wallet_address must be a valid Ethereum address (0x
                      followed by 40 hex characters)
              examples:
                missing_wallet:
                  summary: Missing wallet address
                  value:
                    error: Missing parameter
                    message: wallet_address is required
                invalid_wallet:
                  summary: Invalid wallet address format
                  value:
                    error: Invalid wallet address
                    message: >-
                      wallet_address must be a valid Ethereum address (0x
                      followed by 40 hex characters)
                invalid_limit:
                  summary: Invalid limit parameter
                  value:
                    error: Invalid limit parameter
                    message: limit must be a number between 1 and 100

````

Built with [Mintlify](https://mintlify.com).