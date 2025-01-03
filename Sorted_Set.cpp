#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "Sorted_Set.hpp"
#include "usual.hpp"


struct HKey {
    HNode node;
    const char *name = NULL;
    size_t len = 0;
};

static void ssnode_del(SSNode *node) {
    free(node);
}

static size_t min_node(size_t lhs, size_t rhs) {
    return lhs < rhs ? lhs : rhs;
}


static SSNode *ssnode_new(const char *name, size_t len, double score) {
    SSNode *node = (SSNode *)malloc(sizeof(SSNode) + len);
    assert(node);  
    avl_init(&node->tree);
    node->hmap.next = NULL;
    node->hmap.hashcode = str_hash((uint8_t *)name, len);
    node->score = score;
    node->len = len;
    memcpy(&node->name[0], name, len);
    return node;
}

static bool ssless(AVLNode *lhs, AVLNode *rhs) {
    SSNode *zr = container_of(rhs, SSNode, tree);
    return ssless(lhs, zr->score, zr->name, zr->len);
}


static bool ssless(AVLNode *lhs, double score, const char *name, size_t len){
    SSNode *zl = container_of(lhs, SSNode, tree);
    if (zl->score != score) {
        return zl->score < score;
    }
    int rv = memcmp(zl->name, name, min_node(zl->len, len));
    if (rv != 0) {
        return rv < 0;
    }
    return zl->len < len;
}

static void tree_insert(Sorted_Set *sset, SSNode *node) {
    AVLNode *parent = NULL;         
    AVLNode **from = &sset->root;   
    while (*from) {                 
        parent = *from;
        from = ssless(&node->tree, parent) ? &parent->left : &parent->right;
    }
    *from = &node->tree;            
    node->tree.parent = parent;
    sset->root = avl_fix(&node->tree);
}


static void sset_update(Sorted_Set *sset, SSNode *node, double score) {
    if (node->score == score) {
        return;
    }
    sset->root = avl_del(&node->tree);
    avl_init(&node->tree);
    node->score = score;
    tree_insert(sset, node);
}

bool sset_insert(Sorted_Set *sset, const char *name, size_t len, double score) {
    SSNode *node = sset_lookup(sset, name, len);
    if (node) {
        sset_update(sset, node, score);
        return false;
    } else {
        node = ssnode_new(name, len, score);
        hm_insert(&sset->hmap, &node->hmap);
        tree_insert(sset, node);
        return true;
    }
}


void sset_delete(Sorted_Set *sset, SSNode *node) {
    
    HKey key;
    key.node.hashcode = node->hmap.hashcode;
    key.name = node->name;
    key.len = node->len;
    HNode *found = hm_delete(&sset->hmap, &key.node, &hcmp);
    assert(found);
    
    sset->root = avl_del(&node->tree);
    ssnode_del(node);
}


static bool hcmp(HNode *node, HNode *key) {
    SSNode *ssnode = container_of(node, SSNode, hmap);
    HKey *hkey = container_of(key, HKey, node);
    if (ssnode->len != hkey->len) {
        return false;
    }
    return 0 == memcmp(ssnode->name, hkey->name, ssnode->len);
}

SSNode *sset_lookup(Sorted_Set *sset, const char *name, size_t len) {
    if (!sset->root) {
        return NULL;
    }

    HKey key;
    key.node.hashcode = str_hash((uint8_t *)name, len);
    key.name = name;
    key.len = len;
    HNode *found = hm_lookup(&sset->hmap, &key.node, &hcmp);
    return found ? container_of(found, SSNode, hmap) : NULL;
}


SSNode *sset_seekge(Sorted_Set *sset, double score, const char *name, size_t len) {
    AVLNode *found = NULL;
    for (AVLNode *node = sset->root; node; ) {
        if (ssless(node, score, name, len)) {
            node = node->right; 
        } else {
            found = node;       
            node = node->left;
        }
    }
    return found ? container_of(found, SSNode, tree) : NULL;
}


SSNode *ssnode_offset(SSNode *node, int64_t offset) {
    AVLNode *tnode = node ? avl_off_set(&node->tree, offset) : NULL;
    return tnode ? container_of(tnode, SSNode, tree) : NULL;
}

static void tree_dispose(AVLNode *node) {
    if (!node) {
        return;
    }
    tree_dispose(node->left);
    tree_dispose(node->right);
    ssnode_del(container_of(node, SSNode, tree));
}


void sset_clear(Sorted_Set *sset) {
    hm_clear(&sset->hmap);
    tree_dispose(sset->root);
    sset->root = NULL;
}
