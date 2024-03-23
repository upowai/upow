# uPow

    **What is uPow ?**
    uPow, which stands for Useful Power of Work, is a Layer1 implementation that's been launched in a fair manner, closely following the original Nakamoto consensus. It's a protocol crafted for decentralized inodes. These inodes exist to create decentralized Useful work. Each inode symbolizes a market driven by incentives, competing to deliver the best decentralized Usefulwork.

    uPow adopts a distinctive approach to the conventional blockchain. Rather than utilizing computational power for solving arbitrary puzzles, we channel this power into accomplishing useful tasks. We've developed a consensus mechanism that draws inspiration from PoW (Proof of Work), DPoS (Delegated Proof of Stake), and PoA (Proof of Authority).

    Inode miners receive their rewards in the form of uPow coins. You have the option to set up your own Inode, tailoring its incentive mechanism, to kick off your own inode, or you can become part of an existing Inode within the uPow ecosystem. Take, for instance, Inode1, an image detection Inode. It offers incentives to miners who contribute computational power for the creation of models.

## Key Specifications

- **Maximum Supply:** The total supply of uPow is capped at 18,884,643 units, ensuring a deflationary model to preserve value over time.
- **Decimal Precision:** uPow supports up to 8 decimal places, allowing for micro-transactions and enhancing user flexibility in transaction amounts.
- **Block Generation:** Blocks in the uPow blockchain are generated approximately every minute, striking a balance between transaction confirmation times and network efficiency.
- **Block Size Limit:** Each block has a maximum size limit of 2MB, optimizing the network’s throughput while maintaining speed and reliability.
- **Transactions Per Block:** Given the average transaction size of 250 bytes, with a typical structure of 5 inputs and 2 outputs, a single block can accommodate approximately 8300 transactions.
- **Transaction Throughput:** The network is designed to handle approximately 40 transactions per second, ensuring scalability and performance for users.

## Installation Steps

### 1. Install Python 3.8.9

The uPow blockchain requires Python version 3.8.9. Use the provided Makefile to automate the installation process.

```bash
make -f makefile.python3.8.9 install_python3.8.9
```

This command will update your system, install necessary dependencies, download Python 3.8.9, compile it, and perform the installation.

### 2. Install PostgreSQL 14

PostgreSQL 14 is required for the database component of the uPow blockchain. Use the second Makefile to install and start PostgreSQL.

```bash
make -f makefile.postgres all
```

This will import the PostgreSQL GPG key, add the PostgreSQL repository, install PostgreSQL 14, and ensure the service is started and enabled to run at boot.

### 3. Database Setup

After installing the required software, run the `db_setup.sh` script to create the uPow database and configure PostgreSQL.

First, make the script executable:

```bash
chmod +x db_setup.sh
```

Then, execute the script:

```bash
./db_setup.sh
```

This script installs additional required packages, creates the uPow database and user, sets permissions, and imports the initial database schema.

```bash
git clone https://github.com/upowai/upow
cd upow
pip install -r requirements.txt
uvicorn upow.node.main:app --port 3006
```

Node should now sync the blockchain and start working

## Mining

uPow uses a PoW system.

Block hash algorithm is sha256.  
The block sha256 hash must start with the last `difficulty` hex characters of the previously mined block.  
`difficulty` can also have decimal digits, that will restrict the `difficulty + 1`th character of the derived sha to have a limited set of values.

```python
from math import ceil

difficulty = 6.3
decimal = difficulty % 1

charset = '0123456789abcdef'
count = ceil(16 * (1 - decimal))
allowed_characters = charset[:count]
```

Address must be present in the string in order to ensure block property.

Blocks have a block reward that will half itself til it reaches 0.  
There will be `150000` blocks with reward `100`, and so on til `0.390625`, which will last `458732` blocks.  
The last block with a reward will be the `458733`th, with a reward of `0.3125`.  
Subsequent blocks won't have a block reward.  
Reward will be added the fees of the transactions included in the block.  
A transaction may also have no fees at all.
