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

  return FrontendInterface::handleFrontend(argc, argv);
}
