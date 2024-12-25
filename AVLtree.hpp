#pragma once

#include <stddef.h>
#include <stdint.h>


struct AVLNode {
    AVLNode *parent = NULL;
    AVLNode *left = NULL;
    AVLNode *right = NULL;
    uint32_t height = 0;    
    uint32_t cnt = 0;       
};

