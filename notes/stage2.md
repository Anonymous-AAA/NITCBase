# Internal Index Block Structure

- Each block stores a maximum of 100 attribute values
- Each of these values has an associated pair of left and right child pointers
- There will be 101 child pointers
- First 32 bytes store the header followed by actual attribute key values and child pointers
- Header similar to that of a record block.
- Child pointer is of 4 bytes and attribute key of 16 bytes
- 12 bytes remain unused

# REFER DOC for rest

# Important points

- Each slot stores a single record of a relation.
- Maximum number of relations allowed in NITCBase is 18

# Maybe errors
- Caution at the end of the stage the total number of attributes can be 20 not till 19
