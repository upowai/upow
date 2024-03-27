# Blockchain API Endpoints

### Push Transaction

- **URL**: `/push_tx`
- **Method**: `GET` | `POST`
- **Description**: Submit a new transaction to the blockchain.
- **Parameters**:
  - `tx_hex`: The hexadecimal representation of the transaction.

### Push Block

- **URL**: `/push_block`
- **Method**: `GET` | `POST`
- **Description**: Submit a new block to the blockchain.
- **Parameters**:
  - `block_content`: The content of the block.
  - `txs`: Transactions included in the block.
  - `block_no`: The block number.

### Synchronize Blockchain

- **URL**: `/sync_blockchain`
- **Method**: `GET`
- **Description**: Synchronize the local blockchain with the network.
- **Rate Limit**: 10 requests per minute.

### Get Mining Information

- **URL**: `/get_mining_info`
- **Method**: `GET`
- **Description**: Retrieve information necessary for mining, including difficulty and pending transactions.

### Get Validators Information

- **URL**: `/get_validators_info`
- **Method**: `GET`
- **Description**: Get detailed information about validators in the network.

### Get Delegates Information

- **URL**: `/get_delegates_info`
- **Method**: `GET`
- **Description**: Get detailed information about delegates in the network.

### Get Address Information

- **URL**: `/get_address_info`
- **Method**: `GET`
- **Description**: Retrieve detailed information about a specific blockchain address, including balance, stake, transactions, and more.
- **Parameters**:
  - `address`: The blockchain address to query.
  - `transactions_count_limit`: Maximum number of transactions to return (default is 5, maximum is 50).
  - `page`: Pagination page number (default is 1).
  - `show_pending`: Whether to show pending transactions (default is false).
  - `verify`: Whether to verify transactions (default is false).
  - `stake_outputs`: Whether to include stake outputs (default is false).
  - `delegate_spent_votes`: Whether to include delegate spent votes (default is false).
  - `delegate_unspent_votes`: Whether to include delegate unspent votes (default is false).
  - `address_state`: Whether to include the address state (default is false).
  - `inode_registration_outputs`: Whether to include inode registration outputs (default is false).
  - `validator_unspent_votes`: Whether to include validator unspent votes (default is false).
  - `validator_spent_votes`: Whether to include validator spent votes (default is false).
- **Rate Limit**: 10 requests per second.

### Add Node

- **URL**: `/add_node`
- **Method**: `GET`
- **Description**: Add a new node to the network.
- **Parameters**:
  - `url`: The URL of the node to add.
- **Rate Limit**: 10 requests per minute.

### Get Nodes

- **URL**: `/get_nodes`
- **Method**: `GET`
- **Description**: Retrieve a list of recent nodes in the network.

### Get Pending Transactions

- **URL**: `/get_pending_transactions`
- **Method**: `GET`
- **Description**: Retrieve a list of pending transactions.

### Get Transaction

- **URL**: `/get_transaction`
- **Method**: `GET`
- **Description**: Retrieve detailed information about a specific transaction.
- **Parameters**:
  - `tx_hash`: The hash of the transaction to query.
  - `verify`: Whether to verify the transaction (default is false).
- **Rate Limit**: 2 requests per second.

### Get Block

- **URL**: `/get_block`
- **Method**: `GET`
- **Description**: Retrieve detailed information about a specific block.
- **Parameters**:
  - `block`: The block number or hash to query.
  - `full_transactions`: Whether to include full transaction details (default is false).
- **Rate Limit**: 30 requests per minute.

### Get Block Details

- **URL**: `/get_block_details`
- **Method**: `GET`
- **Description**: Retrieve detailed information and a list of transactions for a specific block.
- **Parameters**:
  - `block`: The block number or hash to query.
- **Rate Limit**: 10 requests per minute.

### Get Blocks

- **URL**: `/get_blocks`
- **Method**: `GET`
- **Description**: Retrieve a list of blocks from the blockchain.
- **Parameters**:
  - `offset`: The starting index from which to retrieve blocks.
  - `limit`: The maximum number of blocks to retrieve (default is set by server, maximum is 1000).
- **Rate Limit**: 10 requests per minute.

### Get Blocks Details

- **URL**: `/get_blocks_details`
- **Method**: `GET`
- **Description**: Retrieve detailed information about a range of blocks, including transactions within each block.
- **Parameters**:
  - `offset`: The starting index from which to retrieve block details.
  - `limit`: The maximum number of block details to retrieve (default is set by server, maximum is 1000).
- **Rate Limit**: 10 requests per minute.

### Dobby Info

- **URL**: `/dobby_info`
- **Method**: `GET`
- **Description**: Retrieve information about active inodes, including their emission rates.
- **Rate Limit**: 10 requests per minute.

### Get Supply Information

- **URL**: `/get_supply_info`
- **Method**: `GET`
- **Description**: Retrieve information about the blockchain's supply, including the maximum supply and circulating supply.
- **Rate Limit**: 10 requests per minute.
