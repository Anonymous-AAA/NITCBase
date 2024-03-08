# Doubts
1. However, in a B+ Tree, when a leaf node becomes full and is split, the middle entry is stored in both the parent node and the leaf node. In the case of a split in the internal node, the middle entry is stored only in the parent similar to B Tree. Is this correct ?
A. Yes it is

2. Bplustree line 784 doubt , same doubt at create new root function
3. Bplustree BPlusTree::splitInternal why are we getting the type. int type= ??

# Notes
1.Bplustree::bPlusDestroy function is called when

    i)   the user issues the DROP INDEX command
    ii)  in a situation where no further disk blocks can be allotted during the creation of/insertion to a B+ Tree
    iii) while deleting an entire relation in NITCbase
2.In Bplustree::bplusinsert The caller is expected to ensure that

    i)   the RecId passed belongs to a valid record in the same relation
    ii)  a duplicate index entry for this record does not already exist in the B+ tree.
    iii) recId actually points to the specific record that the argument attribute value belongs to

This function will add the pair (attrVal, recId) to the B+ tree without any validation on these arguments.

# Errors
1. In documentation they missed to update the attrCache entry for rootblock in BPlusTree::bPlusCreate
