#include "Buffer/StaticBuffer.h"
#include "Cache/AttrCacheTable.h"
#include "Cache/OpenRelTable.h"
#include "Cache/RelCacheTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include "define/constants.h"
#include <iostream>
#include <string.h>

int main(int argc, char *argv[]) {

  // creating run copy of the disk
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  for (int i = 0; i <= 1; i++) {

    RelCatEntry relCatBuf;

    RelCacheTable::getRelCatEntry(i, &relCatBuf);
    printf("Relation: %s\n", relCatBuf.relName);

    for (int j = 0; j < relCatBuf.numAttrs; j++) {

      AttrCatEntry attrCatBuf;

      AttrCacheTable::getAttrCatEntry(i, j, &attrCatBuf);
      printf("  %s: %s\n", attrCatBuf.attrName,
             attrCatBuf.attrType == NUMBER ? "NUM" : "STR");
    }
  }

  // // create objects for the relation catalog
  // RecBuffer relCatBuffer(RELCAT_BLOCK);
  //
  // HeadInfo relCatHeader;
  // HeadInfo attrCatHeader;
  //
  // // load the header of the RELCAT block into relCatHeader
  // // (we will implement these functions later)
  // relCatBuffer.getHeader(&relCatHeader);
  //
  // for (int i = 0; i < relCatHeader.numEntries; i++) {
  //
  //   // attribute array to store a record
  //   Attribute relCatRecord[RELCAT_NO_ATTRS]; // will store the record from
  //   the
  //                                            // relation catalog
  //
  //   relCatBuffer.getRecord(relCatRecord, i);
  //
  //   printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);
  //
  //   // create object for the  attribute catalog
  //   // nextblock to be read in attribute catalog
  //   int nextBlock = ATTRCAT_BLOCK;
  //
  //   while (nextBlock != INVALID_BLOCKNUM) {
  //
  //     // create objects for the attribute catalog
  //     RecBuffer attrCatBuffer(nextBlock);
  //     // load the header of the ATTRCAT block into attrCatHeader
  //     attrCatBuffer.getHeader(&attrCatHeader);
  //     for (int j = 0; j < attrCatHeader.numEntries; j++) {
  //
  //       // declare attrCatRecord and load the attribute catalog entry into it
  //       Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  //
  //       attrCatBuffer.getRecord(attrCatRecord, j);
  //
  //       if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,
  //                  relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0) {
  //         const char *attrType =
  //             attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM"
  //                                                                   : "STR";
  //         printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,
  //                attrType);
  //       }
  //     }
  //
  //     nextBlock = attrCatHeader.rblock;
  //   }
  //
  //   printf("\n");
  // }

  return 0;
}
