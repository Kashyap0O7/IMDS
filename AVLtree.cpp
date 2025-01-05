#include <assert.h>

#include "AVLtree.hpp"

static uint32_t max_node(uint32_t lhs, uint32_t rhs) {
    return lhs < rhs ? rhs : lhs;
}

static void avl_update(AVLNode *node) {
    node->height = 1 + max_node(avl_height(node->left), avl_height(node->right));
    node->cnt = 1 + avl_cnt(node->left) + avl_cnt(node->right);
}

static AVLNode *rotate_right(AVLNode *node) {
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->left;
    AVLNode *inner = new_node->right;

    node->left = inner;
    if (inner) {
        inner->parent = node;
    }
    new_node->parent = parent;
    new_node->right = node;
    node->parent = new_node;
    
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *rotate_left(AVLNode *node) {
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->right;
    AVLNode *inner = new_node->left;

    node->right = inner;
    if (inner) {
        inner->parent = node;
    
    }
    new_node->parent = parent;
    new_node->left = node;
    node->parent = new_node;
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *avl_fix_right(AVLNode *node){
    if (avl_height(node->right->right) < avl_height(node->right->left)) {
        node->right = rotate_right(node->right);
    }

    return rotate_left(node);
}

static AVLNode *avl_fix_left(AVLNode *node) {
    if (avl_height(node->left->left) < avl_height(node->left->right)) {
        node->left = rotate_left(node->left);
    }
    return rotate_right(node);
}

AVLNode *avl_fix(AVLNode *node) {
    while (true) {
        AVLNode **from = &node;
        AVLNode *parent = node->parent;

        if (parent) {
            from = ((parent->left == node) ? (&parent->left) : (&parent->right));
        }

        avl_update(node);
        uint32_t le = avl_height(node->left);
        uint32_t ri = avl_height(node->right);

        if (le == ri + 2) {
            *from = avl_fix_left(node);
        } else if (le + 2 == ri) {
            *from = avl_fix_right(node);
        }


        if (!parent) {
            return *from;
        }
        node = parent;
    }
}

AVLNode *avl_off_set(AVLNode *node, int64_t offset) {
    int64_t pos = 0;
    while (offset != pos) {
        if (pos < offset && pos + avl_cnt(node->right) >= offset) {
            node = node->right;
            pos += avl_cnt(node->left) + 1;
        } 
        else if(pos > offset && pos - avl_cnt(node->left) <= offset) {
            node = node->left;
            pos -= avl_cnt(node->right) + 1;
        } 
        else{
            AVLNode *parent = node->parent;
            if (!parent) return NULL;
            if (parent->right == node) {
                pos -= avl_cnt(node->left) + 1;
            } else {
                pos += avl_cnt(node->right) + 1;
            }
            node = parent;
        }
    }

    return node;
}

static AVLNode *avl_lazy_del(AVLNode *node) {
    assert(!node->left || !node->right);

    AVLNode *child = node->left ? node->left : node->right;
    AVLNode *parent = node->parent;

    if (child) {
        child->parent = parent;
    }
    if (!parent) {
        return child;
    }

    AVLNode **from = parent->left == node ? &parent->left : &parent->right;

    *from = child;
    return avl_fix(parent);
}

AVLNode *avl_del(AVLNode *node) {
    if (!node->left || !node->right) return avl_lazy_del(node);
    AVLNode *victim = node->right;

    while (victim->left) {
        victim = victim->left;
    }

    AVLNode *root = avl_lazy_del(victim);
    *victim = *node;

    if (victim->left) {
        victim->left->parent = victim;
    }
    if (victim->right) {
        victim->right->parent = victim;
    }

    AVLNode **from = &root;
    AVLNode *parent = node->parent;

    if (parent) {
        from = parent->left == node ? &parent->left : &parent->right;
    }

    *from = victim;
    return root;
}
