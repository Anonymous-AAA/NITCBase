- XFS disk is divided into blocks
- Two types of blocks :
    - Record blocks
    - Index blocks
    
    Index blocks are classified as :
        - Leaf index blocks
        - Internal index blocks

# Disk Model
- Disk is a sequence of blocks
- A Block is a sequence of bytes
- Disk consists of 8192 blocks
- Each block is of 2048 bytes
- So total 16MB of storage
- Disk blocks are indexed from 0 to 8191

- Block Allocation Map tells whether a particular block is free or occupied
- If occupied it stores the type of the block.
- It requires 1 byte/block
- 8192/2048=4 so 4 blocks are required for block allocation map
- Blocks 0-3 reserved for Block allocation map, whereas Blocks 4 and 5 are reserved for storing the block of Relation Catalog and the first block of Attribute Catalog, respectively

# Disk Class
- NITCBase is a collection of relations and each relation is a collection of records
- Each relation is stored as a set of blocks in disk organized as a linked list.
- Each such block is called a record block.
- Records consists of attributes of fixed size - 16 bytes, but the records of a relation can be of variable size (from 16 bytes to largest record that can fit into a block)
- Each record block is divided into slots of variable record size, each slot stores a single record.
- Size of a slot is 16*k bytes where k is the number of attributes.







