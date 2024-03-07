#include "OpenRelTable.h"
#include "AttrCacheTable.h"
#include "RelCacheTable.h"

#include <cstdlib>
#include <cstring>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  // all entries in tableMetaInfo to be free
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    OpenRelTable::tableMetaInfo[i].free = true;
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

  struct AttrCacheEntry *attrCacheEntry = nullptr;
  struct AttrCacheEntry *prev = nullptr;
  struct AttrCacheEntry *head = nullptr;

  for (int slotNum = 0; slotNum < RELCAT_NO_ATTRS; slotNum++) {

    attrCacheEntry = (struct AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    attrCatBlock.getRecord(attrCatRecord, slotNum);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord,
                                         &attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = slotNum;

    if (head == nullptr) {
      head = attrCacheEntry;
    } else {
      prev->next = attrCacheEntry;
    }

    prev = attrCacheEntry;
  }

  // set the next field in the last entry to nullptr
  attrCacheEntry->next = nullptr;

  AttrCacheTable::attrCache[RELCAT_RELID] = head;

  /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/

  // set up the attributes of the attribute cache similarly.
  // read slots 6-11 from attrCatBlock and initialise recId appropriately
  head = nullptr;
  attrCacheEntry = nullptr;

  for (int slotNum = RELCAT_NO_ATTRS;
       slotNum < RELCAT_NO_ATTRS + ATTRCAT_NO_ATTRS; slotNum++) {

    attrCacheEntry = (struct AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    attrCatBlock.getRecord(attrCatRecord, slotNum);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord,
                                         &attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = slotNum;

    if (head == nullptr) {
      head = attrCacheEntry;
    } else {
      prev->next = attrCacheEntry;
    }

    prev = attrCacheEntry;
  }

  // set the next field in the last entry to nullptr
  attrCacheEntry->next = nullptr;

  // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;

  /************ Setting up tableMetaInfo entries ************/

  // in the tableMetaInfo array
  //   set free = false for RELCAT_RELID and ATTRCAT_RELID
  //   set relname for RELCAT_RELID and ATTRCAT_RELID
  OpenRelTable::tableMetaInfo[RELCAT_RELID].free = false;
  OpenRelTable::tableMetaInfo[ATTRCAT_RELID].free = false;

  strcpy(OpenRelTable::tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
  strcpy(OpenRelTable::tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
}

OpenRelTable::~OpenRelTable() {

  // close all open relations (from rel-id = 2 onwards. Why?)
  for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i); // we will implement this function later
    }
  }

  /**** Closing the catalog relations in the relation cache ****/

  // releasing the relation cache entry of the attribute catalog

  /* RelCatEntry of the ATTRCAT_RELID-th RelCacheEntry has been modified */
  if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty) {

    /* Get the Relation Catalog entry from RelCacheTable::relCache
    Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RelCatEntry relCatEntry =
        RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry;

    RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

    RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;

    // declaring an object of RecBuffer class to write back to the buffer
    RecBuffer relCatBlock(recId.block);

    // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    relCatBlock.setRecord(relCatRecord, recId.slot);
  }
  // free the memory dynamically allocated to this RelCacheEntry
  free(RelCacheTable::relCache[ATTRCAT_RELID]);

  // releasing the relation cache entry of the relation catalog
  /* RelCatEntry of the RELCAT_RELID-th RelCacheEntry has been modified */
  if (RelCacheTable::relCache[RELCAT_RELID]->dirty) {

    /* Get the Relation Catalog entry from RelCacheTable::relCache
    Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RelCatEntry relCatEntry =
        RelCacheTable::relCache[RELCAT_RELID]->relCatEntry;
    RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

    RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;
    // declaring an object of RecBuffer class to write back to the buffer
    RecBuffer relCatBlock(recId.block);

    // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    relCatBlock.setRecord(relCatRecord, recId.slot);
  }
  // free the memory dynamically allocated for this RelCacheEntry
  free(RelCacheTable::relCache[RELCAT_RELID]);

  // free the memory allocated for the attribute cache entries of the
  // relation catalog and the attribute catalog

  // attribute catalog entry changes are not written back to disk
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

/* This function will open a relation having name `relName`.
Since we are currently only working with the relation and attribute catalog, we
will just hardcode it. In subsequent stages, we will loop through all the
relations and open the appropriate one.
*/
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  /* traverse through the tableMetaInfo array,
      find the entry in the Open Relation Table corresponding to relName.*/
  for (int i = 0; i < MAX_OPEN; i++) {
    if (!tableMetaInfo[i].free && // changes made here
        strcmp(relName, OpenRelTable::tableMetaInfo[i].relName) == 0) {
      return i;
    }
  }

  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
  for (int i = 0; i < MAX_OPEN; i++) {

    if (OpenRelTable::tableMetaInfo[i].free)
      return i;
  }

  // if found return the relation id, else return E_CACHEFULL.
  return E_CACHEFULL;
}

// int OpenRelTable::openRel(unsigned char relName[ATTR_SIZE]) {
int OpenRelTable::openRel(char relName[ATTR_SIZE]) {

  int relId = OpenRelTable::getRelId(relName);

  /* the relation `relName` already has an entry in the Open Relation Table */
  // (checked using OpenRelTable::getRelId())
  if (relId != E_RELNOTOPEN) {

    // return that relation id;
    return relId;
  }

  /* find a free slot in the Open Relation Table
     using OpenRelTable::getFreeOpenRelTableEntry(). */
  relId = OpenRelTable::getFreeOpenRelTableEntry();

  /* free slot not available */
  if (relId == E_CACHEFULL) {
    return E_CACHEFULL;
  }

  // let relId be used to store the free slot.
  // int relId;

  /****** Setting up Relation Cache entry for the relation ******/

  /* search for the entry with relation name, relName, in the Relation Catalog
     using BlockAccess::linearSearch(). Care should be taken to reset the
     searchIndex of the relation RELCAT_RELID before calling linearSearch().*/
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  // relcatRecId stores the rec-id of the relation `relName` in the Relation
  // Catalog.
  RecId relcatRecId;
  Attribute attrVal;
  strcpy(attrVal.sVal, relName);
  relcatRecId =
      BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, attrVal, EQ);

  /* relcatRecId == {-1, -1} */
  if (relcatRecId.slot == -1 && relcatRecId.block == -1) {
    // (the relation is not found in the Relation Catalog.)
    return E_RELNOTEXIST;
  }

  /* read the record entry corresponding to relcatRecId and create a
    relCacheEntry on it using RecBuffer::getRecord() and
    RelCacheTable::recordToRelCatEntry(). update the recId field of this
    Relation Cache entry to relcatRecId. use the Relation Cache entry to set the
    relId-th entry of the RelCacheTable. NOTE: make sure to allocate memory for
    the RelCacheEntry using malloc()
  */
  Attribute relCatRec[RELCAT_NO_ATTRS];
  RecBuffer recBuffer(RELCAT_BLOCK);
  int ret = recBuffer.getRecord(relCatRec, relcatRecId.slot);

  if (ret != SUCCESS) {
    return ret;
  }

  RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRec, &relCacheEntry.relCatEntry);
  relCacheEntry.recId = relcatRecId;
  RelCacheTable::relCache[relId] =
      (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[relId]) = relCacheEntry;

  /****** Setting up Attribute Cache entry for the relation ******/

  // let listHead be used to hold the head of the linked list of attrCache
  // entries.
  AttrCacheEntry *listHead = nullptr;
  AttrCacheEntry *attrCacheEntry = nullptr;
  AttrCacheEntry *prev = nullptr;

  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of
  BlockAccess::linearSearch() care should be taken to reset the searchIndex of
  the relation, ATTRCAT_RELID, corresponding to Attribute Catalog before the
  first call to linearSearch().*/
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  RecId attrcatRecId;
  attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME,
                                           attrVal, EQ);
  while (attrcatRecId.block != -1 && attrcatRecId.slot != -1) {
    /* let attrcatRecId store a valid record id an entry of the relation,
    relName, in the Attribute Catalog.*/
    // RecId attrcatRecId;

    /* read the record entry corresponding to attrcatRecId and create an
    Attribute Cache entry on it using RecBuffer::getRecord() and
    AttrCacheTable::recordToAttrCatEntry().
    update the recId field of this Attribute Cache entry to attrcatRecId.
    add the Attribute Cache entry to the linked list of listHead .*/
    // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
    Attribute attrCatRec[ATTRCAT_NO_ATTRS];
    RecBuffer recBuffer(attrcatRecId.block);
    int ret = recBuffer.getRecord(attrCatRec, attrcatRecId.slot);

    if (ret != SUCCESS) {
      return ret;
    }

    attrCacheEntry = (struct AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));

    AttrCacheTable::recordToAttrCatEntry(attrCatRec,
                                         &attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId = attrcatRecId;

    if (listHead == nullptr) {
      listHead = attrCacheEntry;
    } else {
      prev->next = attrCacheEntry;
    }

    prev = attrCacheEntry;
    attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID,
                                             ATTRCAT_ATTR_RELNAME, attrVal, EQ);
  }

  // set the next field in the last entry to nullptr
  attrCacheEntry->next = nullptr;

  // set the relIdth entry of the AttrCacheTable to listHead.
  AttrCacheTable::attrCache[relId] = listHead;

  /****** Setting up metadata in the Open Relation Table for the relation******/

  // update the relIdth entry of the tableMetaInfo with free as false and
  OpenRelTable::tableMetaInfo[relId].free = false;
  // relName as the input.
  strcpy(OpenRelTable::tableMetaInfo[relId].relName, relName);

  return relId;
}

int OpenRelTable::closeRel(int relId) {
  /* rel-id corresponds to relation catalog or attribute catalog*/
  if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
    return E_NOTPERMITTED;
  }

  /* 0 <= relId < MAX_OPEN */
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  /* rel-id corresponds to a free slot*/
  if (OpenRelTable::tableMetaInfo[relId].free) {
    return E_RELNOTOPEN;
  }

  /****** Releasing the Relation Cache entry of the relation ******/
  /*If RelCatEntry of the relId-th Relation Cache entry has been modified */
  if (RelCacheTable::relCache[relId]->dirty) {

    /* Get the Relation Catalog entry from RelCacheTable::relCache
    Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
    RelCatEntry relCatEntry;
    relCatEntry = RelCacheTable::relCache[relId]->relCatEntry;
    Attribute record[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&relCatEntry, record);
    RecId recId = RelCacheTable::relCache[relId]->recId;

    // declaring an object of RecBuffer class to write back to the buffer
    RecBuffer relCatBlock(recId.block);

    // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    relCatBlock.setRecord(record, recId.slot);
  }

  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() function
  free(RelCacheTable::relCache[relId]);

  /****** Releasing the Attribute Cache entry of the relation ******/

  // for all the entries in the linked list of the relIdth Attribute Cache
  // entry.
  AttrCacheEntry *next = nullptr;

  for (AttrCacheEntry *entry = AttrCacheTable::attrCache[relId];
       entry != nullptr; entry = next) {
    // if the entry has been modified:
    if (entry->dirty) {
      /* Get the Attribute Catalog entry from attrCache
       Then convert it to a record using AttrCacheTable::attrCatEntryToRecord().
       Write back that entry by instantiating RecBuffer class. Use recId
       member field and recBuffer.setRecord() */
      AttrCatEntry attrCatEntry = entry->attrCatEntry;
      Attribute record[ATTRCAT_NO_ATTRS];
      AttrCacheTable::attrCatEntryToRecord(&attrCatEntry, record);
      RecBuffer buffer(entry->recId.block);
      buffer.setRecord(record, entry->recId.slot);
    }

    // free the memory dynamically alloted to this entry in Attribute
    // Cache linked list and assign nullptr to that entry
    next = entry->next;
    free(entry);
  }

  // update `tableMetaInfo` to set `relId` as a free slot
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr
  OpenRelTable::tableMetaInfo[relId].free = true;
  RelCacheTable::relCache[relId] = nullptr;
  AttrCacheTable::attrCache[relId] = nullptr;

  return SUCCESS;
}
