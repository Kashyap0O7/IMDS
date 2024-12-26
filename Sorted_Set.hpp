#pragma once

#include "hashtable.hpp"
#include "AVLtree.hpp"


struct Sorted_Set {
    AVLNode *root = NULL;  
    HMap hmap;              
};

struct SSNode {
    AVLNode tree;
    HNode hmap;
    double score = 0;
    size_t len = 0;
    char name[0];       
};
