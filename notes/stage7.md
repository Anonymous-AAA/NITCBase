# Doubts
1. Why use characters for block types ? why not macros? Answer: As it collides with the constructor 2 defintion


# Errata
1. RelCacheTable :: relCatEntryToRecord :- argument record of type Attribute[RELCAT_SIZE] should be Attribute[RELCAT_NO_ATTRS]
2. In openreltable.cpp in getRelId check for whether the relation is open is done by checking metainfo free (not given in documentation)


