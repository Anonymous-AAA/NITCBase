#include "OpenRelTable.h"
#include "AttrCacheTable.h"
#include "RelCacheTable.h"

#include <cstdlib>
#include <cstring>

OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
  }

  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Relation Cache Table****/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  // allocate this on the heap because we want it to persist outside this
  // function
  RelCacheTable::relCache[RELCAT_RELID] =
      (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  /**** setting up Attribute Catalog relation in the Relation Cache Table ****/

  // set up the relation cache entry for the attribute catalog similarly
  // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

  // set the value at RelCacheTable::relCache[ATTRCAT_RELID]
  RelCacheTable::relCache[ATTRCAT_RELID] =
      (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;

  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  // iterate through all the attributes of the relation catalog and create a
  // linked list of AttrCacheEntry (slots 0 to 5) for each of the entries, set
  //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
  //    attrCacheEntry.recId.slot = i   (0 to 5)
  //    and attrCacheEntry.next appropriately
  // NOTE: allocate each entry dynamically using malloc

  struct AttrCacheEntry attrCacheEntry;
  struct AttrCacheEntry *prev = nullptr;
  struct AttrCacheEntry *curr = nullptr;
  struct AttrCacheEntry *head = nullptr;

  for (int slotNum = 0; slotNum < RELCAT_NO_ATTRS; slotNum++) {

    attrCatBlock.getRecord(attrCatRecord, slotNum);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord,
                                         &attrCacheEntry.attrCatEntry);
    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    attrCacheEntry.recId.slot = slotNum;

    curr = (struct AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    *curr = attrCacheEntry;

    if (head == nullptr) {
      head = curr;
    } else {
      prev->next = curr;
    }

    prev = curr;
  }

  // set the next field in the last entry to nullptr
  curr->next = nullptr;

  AttrCacheTable::attrCache[RELCAT_RELID] = head;

  /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/

  // set up the attributes of the attribute cache similarly.
  // read slots 6-11 from attrCatBlock and initialise recId appropriately
  head = nullptr;

  for (int slotNum = RELCAT_NO_ATTRS;
       slotNum < RELCAT_NO_ATTRS + ATTRCAT_NO_ATTRS; slotNum++) {
    attrCatBlock.getRecord(attrCatRecord, slotNum);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord,
                                         &attrCacheEntry.attrCatEntry);
    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    attrCacheEntry.recId.slot = slotNum;

    curr = (struct AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    *curr = attrCacheEntry;

    if (head == nullptr) {
      head = curr;
    } else {
      prev->next = curr;
    }

    prev = curr;
  }

  // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
}

OpenRelTable::~OpenRelTable() {
  // free all the memory that you allocated in the constructor

  free(RelCacheTable::relCache[RELCAT_RELID]);
  free(RelCacheTable::relCache[ATTRCAT_RELID]);

  struct AttrCacheEntry *curr;
  struct AttrCacheEntry *next;

  curr = AttrCacheTable::attrCache[RELCAT_RELID];

  while (curr) {
    next = curr->next;
    free(curr);
    curr = next;
  }

  curr = AttrCacheTable::attrCache[ATTRCAT_RELID];

  while (curr) {
    next = curr->next;
    free(curr);
    curr = next;
  }
}
