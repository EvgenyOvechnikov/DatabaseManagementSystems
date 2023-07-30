// Evgeny Ovechnikov
// Course: CS540 - Database Management Systems
// Homework: B+tree Index (w5)

#include <string>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <bitset>
#include <map>
using namespace std;

const int NUM_BLOCKS = 3;
const int BLOCK_SIZE = 4096 - 12;   // initialize the block size allowed in main memory according to the question. 12 bytes for overhead.
const int MAX_RECORDS = 65536;      // maximum projected number of records
const uint16_t REC_MEM_SIZE = 28;
const uint16_t REC_PHYS_SIZE = 28;
const uint16_t REC_NAME_ATTR_MAX_LEN = 200;
const uint16_t REC_BIO_ATTR_MAX_LEN = 500;

const string INDEX_FILE_NAME = "EmployeeIndex";
const string DATA_FILE_NAME = "EmployeeRelation";
const int NODE_DEGREE = 4;   // may revise the use of global const later
const int BPLUSTREE_NODE_SIZE = 112;
const int MAGIC_INT_EMPTY = 0x80000000;

int globalNodeId = 0;
uint64_t globalLRU = 0;

// Class representing a tuple from the Employee.csv file
class Record {
public:
    int id, manager_id;
    std::string bio, name;

    Record(vector<std::string> fields) {
        id = stoi(fields[0]);
        name = fields[1];
        bio = fields[2];
        manager_id = stoi(fields[3]);
    }

    Record(uint64_t uid, string uname, string ubio, uint64_t managerId)
    {
        id = uid;
        name = uname;
        bio = ubio;
        manager_id = managerId;
    }

    // Default constructor to make compiler happy
    Record() { }

    int size() {
        return REC_MEM_SIZE + name.length() + bio.length();
    }

    void print() {
        cout << "\tID: " << id << "\n";
        cout << "\tNAME: " << name << "\n";
        cout << "\tBIO: " << bio << "\n";
        cout << "\tMANAGER_ID: " << manager_id << "\n";
    }
};

// Class representing memory block
class Block {
private:
    int nextFree;
    char data[BLOCK_SIZE];

public:
    bool dirty;
    uint64_t lru;

    Block() {
        clearBlock();
    }

    void clearBlock() {
        memset(data, 0, BLOCK_SIZE);
        nextFree = 0;
        lru = globalLRU;
        dirty = false;
    }

    bool insertRecord(Record* rec, int pos) {
        if (BLOCK_SIZE - nextFree < rec->size() ||
            rec->name.length() > REC_NAME_ATTR_MAX_LEN ||
            rec->bio.length() > REC_BIO_ATTR_MAX_LEN)
        {
            return false;
        }

        char* free = data;
        free += nextFree;
        int record_size = REC_MEM_SIZE; // fixed-length header

        // W5 addition: position
        *reinterpret_cast<int*>(free) = static_cast<int>(pos);
        free += sizeof(int);

        // Id
        *reinterpret_cast<uint64_t*>(free) = rec->id;
        free += sizeof(uint64_t);

        // Name Length
        *reinterpret_cast<uint32_t*>(free) = static_cast<uint32_t>(rec->name.length());
        free += sizeof(uint32_t);
        record_size += rec->name.length();

        // Bio Length
        *reinterpret_cast<uint32_t*>(free) = static_cast<uint32_t>(rec->bio.length());
        free += sizeof(uint32_t);
        record_size += rec->bio.length();

        // Manager id
        *reinterpret_cast<uint64_t*>(free) = rec->manager_id;
        free += sizeof(uint64_t);

        // Name
        for (int i = 0; i < rec->name.length(); i++) {
            *reinterpret_cast<char*>(free) = rec->name[i];
            free++;
        }

        // Bio
        for (int i = 0; i < rec->bio.length(); i++) {
            *reinterpret_cast<char*>(free) = rec->bio[i];
            free++;
        }

        nextFree += record_size;
        dirty = true;
        globalLRU++;
        lru = globalLRU;
        return true;
    }

    map<int, Record> getAllRecords() {
        map<int, Record> result;
        for (char* addr = data; addr < data + nextFree; )
        {
            int pos = *reinterpret_cast<int*>(addr);
            addr += sizeof(int);

            uint64_t id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            uint32_t name_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint32_t bio_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint64_t manager_id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            string name(addr, name_len);
            addr += name_len;

            string bio(addr, bio_len);
            addr += bio_len;

            result[pos] = Record(id, name, bio, manager_id);
        }
        return result;
    }

    vector<uint64_t> getAllIds() {
        vector<uint64_t> result;
        for (char* addr = data; addr < data + nextFree; )
        {
            int pos = *reinterpret_cast<int*>(addr);
            addr += sizeof(int);

            uint64_t id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            uint32_t name_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint32_t bio_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            addr += sizeof(uint64_t);
            addr += name_len;
            addr += bio_len;

            result.push_back(id);
        }
        return result;
    }

    Record getRecordById(uint64_t idToGet) {
        for (char* addr = data; addr < data + nextFree; )
        {
            int pos = *reinterpret_cast<int*>(addr);
            addr += sizeof(int);

            uint64_t id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            uint32_t name_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint32_t bio_len = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            uint64_t manager_id = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            string name(addr, name_len);
            addr += name_len;

            string bio(addr, bio_len);
            addr += bio_len;

            if (id == idToGet) {
                return Record(id, name, bio, manager_id);
            }
        }

        // The code should never reach this line. Don't call this method with non-existing Id.
        return Record();
    }
};

class StorageBufferManager {
private:
    // You may declare variables based on your need 
    int lastFreeSpace = MAX_RECORDS * REC_PHYS_SIZE;
    Block* myBuffer;
    string myStorageName;
    fstream storageStream;

    // Save info from the buffer to disk
    void saveRecords(map<int, Record>* records) {
        storageStream.open(myStorageName, ios::binary | ios::in | ios::out);

        // iterating over map
        map<int, Record>::iterator it;
        for (it = records->begin(); it != records->end(); it++) {
            uint32_t nameAddr = lastFreeSpace;
            uint32_t bioAddr = lastFreeSpace + it->second.name.length();
            lastFreeSpace += it->second.name.length() + it->second.bio.length();
            // Casting stuff
            char recordCast[REC_PHYS_SIZE];
            memset(recordCast, 0, REC_PHYS_SIZE);
            char* t = recordCast;
            *reinterpret_cast<uint64_t*>(t) = it->second.id;
            t += sizeof(uint64_t);
            *reinterpret_cast<uint16_t*>(t) = it->second.name.length();
            t += sizeof(uint16_t);
            *reinterpret_cast<uint32_t*>(t) = nameAddr;
            t += sizeof(uint32_t);
            *reinterpret_cast<uint16_t*>(t) = it->second.bio.length();
            t += sizeof(uint16_t);
            *reinterpret_cast<uint32_t*>(t) = bioAddr;
            t += sizeof(uint32_t);
            *reinterpret_cast<uint64_t*>(t) = it->second.manager_id;

            streamoff offset = it->first * REC_PHYS_SIZE;
            storageStream.seekp(offset, ios_base::beg);
            storageStream.write(recordCast, REC_PHYS_SIZE);

            storageStream.seekp(nameAddr, ios_base::beg);
            storageStream.write(it->second.name.c_str(), it->second.name.length());

            storageStream.seekp(bioAddr, ios_base::beg);
            storageStream.write(it->second.bio.c_str(), it->second.bio.length());
        }
        storageStream.close();
    }

    Block* getLeastRecentBlock() {
        Block* result = &myBuffer[0];
        uint64_t leastRecent = globalLRU;
        for (int i = 0; i < NUM_BLOCKS; i++) {
            if (myBuffer[i].lru < leastRecent) {
                leastRecent = myBuffer[i].lru;
                result = &myBuffer[i];
            }
        }
        return result;
    }

public:
    int TotalRecords = 0;   // TODO: Revise the necessity of this variable

    StorageBufferManager(string NewFileName) {
        //initialize your variables
        myBuffer = new Block[NUM_BLOCKS];
        myStorageName = NewFileName;

        // Create your EmployeeRelation file 
        remove(NewFileName.c_str());
        storageStream.open(myStorageName, ios::binary | ios::out);
        storageStream.close();
    }

    ~StorageBufferManager() { }

    // Insert new record 
    void insertRecord(Record record, int pos) {
        //// The block are initialized in constructor
        //myIndexManager.myIndexRecord[record.id] = numRecords;

        // Add record to the block
        bool insertionSuccess = false;
        for (int i = 0; i < NUM_BLOCKS; i++) {
            insertionSuccess = myBuffer[i].insertRecord(&record, pos);
            if (insertionSuccess) {
                break;
            }
        }

        // Take neccessary steps if capacity is reached (you've utilized all the blocks in main memory)
        // if there are no space in buffer, release one page
        if (!insertionSuccess) {
            Block* blockToSave = getLeastRecentBlock();
            map<int, Record> recs = blockToSave->getAllRecords();
            saveRecords(&recs);
            blockToSave->clearBlock();
            blockToSave->insertRecord(&record, pos);
        }
    }

    void saveCache() {
        for (int i = 0; i < NUM_BLOCKS; i++) {
            map<int, Record> recs = myBuffer[i].getAllRecords();
            saveRecords(&recs);
            myBuffer[i].dirty = false;
        }
    }

    // Given an ID, find the relevant record and print it
    // I'm not proud of this code, but have lack of time for optimization...
    Record findRecordById(int id, int pos) {
        // Since this is supposed to be called by Index Manager,
        // we are certain we have the record
        // Looking at cache first
        for (int i = 0; i < NUM_BLOCKS; i++) {
            vector<uint64_t> recIds = myBuffer[i].getAllIds();
            for (int j = 0; j < recIds.size(); j++) {
                if (recIds[j] == id) {
                    return myBuffer[i].getRecordById(recIds[j]);
                }
            }
        }

        // Didn't find in cache, fetching from disk
        // Get the last frequently used page
        Block* blockToReplace = getLeastRecentBlock();
        // Save the block if it's dirty...
        //if (blockToReplace->dirty) {
        //    saveCache();
        //}
        blockToReplace->clearBlock();

        streamoff offset = pos * REC_PHYS_SIZE;
        ifstream ifs;
        //storageStream.open(myStorageName, ios::binary | ios::in);
        ifs.open(myStorageName, ios::binary);

        bool insertionSuccess = false;
        do {
            //storageStream.seekg(offset);
            ifs.seekg(offset);
            char recordCast[REC_PHYS_SIZE];
            char* addr = recordCast;
            //storageStream.read(addr, sizeof(uint64_t));
            ifs.read(addr, sizeof(uint64_t));
            uint64_t uid = *reinterpret_cast<uint64_t*>(addr);
            addr += sizeof(uint64_t);

            //storageStream.read(addr, sizeof(uint16_t));
            ifs.read(addr, sizeof(uint16_t));
            uint16_t nameLength = *reinterpret_cast<uint16_t*>(addr);
            addr += sizeof(uint16_t);

            //storageStream.read(addr, sizeof(uint16_t));
            ifs.read(addr, sizeof(uint32_t));
            uint32_t nameAddr = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            //storageStream.read(addr, sizeof(uint16_t));
            ifs.read(addr, sizeof(uint16_t));
            uint16_t bioLength = *reinterpret_cast<uint16_t*>(addr);
            addr += sizeof(uint16_t);

            //storageStream.read(addr, sizeof(uint16_t));
            ifs.read(addr, sizeof(uint32_t));
            uint32_t bioAddr = *reinterpret_cast<uint32_t*>(addr);
            addr += sizeof(uint32_t);

            //storageStream.read(addr, sizeof(uint64_t));
            ifs.read(addr, sizeof(uint64_t));
            uint64_t managerId = *reinterpret_cast<uint64_t*>(addr);

            char* nameFromCache = (char*)malloc((size_t)nameLength);
            //storageStream.seekg(nameAddr);
            //storageStream.read(nameFromCache, nameLength);
            ifs.seekg(nameAddr);
            ifs.read(nameFromCache, nameLength);

            char* bioFromCache = (char*)malloc((size_t)bioLength);
            //storageStream.read(bioFromCache, bioLength);
            ifs.read(bioFromCache, bioLength);

            Record r = Record(uid, convertToString(nameFromCache, nameLength), convertToString(bioFromCache, bioLength), managerId);

            insertionSuccess = blockToReplace->insertRecord(&r, pos);
            offset += REC_PHYS_SIZE;
        } while (insertionSuccess && offset < TotalRecords * REC_PHYS_SIZE);

        //storageStream.close();
        ifs.close();

        return blockToReplace->getRecordById(id);

        //// If there is record, it would have been returned by now
        //cout << "Record with this Id is not found\n";
        //return Record(id, "Not", "Found", 0);
    }

    // Had to do this, because stream works poorly.
    string convertToString(char* a, int size)
    {
        int i;
        string s = "";
        for (i = 0; i < size; i++) {
            s = s + a[i];
        }
        return s;
    }
};

// Class representing a node in the B+ tree
class BPlusNode {
public:
    vector<int> keys; // Vector to store keys
    vector<BPlusNode*> children; // Vector to store child nodes
    bool is_leaf; // Boolean value indicating whether the node is a leaf node or not
    BPlusNode* parent; // Pointer to the parent node

    // properties I need to save/load node from file
    int localNodeId;
    int parentId;
    // Constructor to initialize the node
    BPlusNode(bool u_is_leaf = false, BPlusNode* u_parent = nullptr)
    {
        is_leaf = u_is_leaf;
        parent = u_parent;
    }

    //Use the Record structure to create necessary data structure to do the following:
    //The pointers in the leaf nodes of the index must keep the addresses of their 
    //corresponding records on the created data file.
    //Record record;
    
    // I use the same vector for storing record pointers as well as child node pointers
    vector<int> addresses;
};

// Class representing the B+ tree
class BPlusTree {
private:
    int myRecordNum = 0;
    StorageBufferManager* manager;

public:
    // Necessary variables
    BPlusNode* root; // Pointer to the root node
    int degree; // Degree of the B+ tree

    // Constructor to initialize the B+ tree
    BPlusTree(int u_degree = 4, BPlusNode* u_root = nullptr)
    {
        degree = u_degree;
        root = u_root;

        manager = new StorageBufferManager(DATA_FILE_NAME);

        // Creating index table file 
        remove(INDEX_FILE_NAME.c_str());
        fstream indexStream;
        indexStream.open(INDEX_FILE_NAME, ios::binary | ios::out);
        indexStream.close();
    }

    // Method to search for a key (id) in the B+ tree to find the index
    BPlusNode* search(int id) {
        BPlusNode* n = root;
        do {
            int p = 0;
            while (p < n->keys.size() - 1 && id >= n->keys[p + 1])
            {
                p++;
            }
            n = getNodeFromDisk(n->addresses[p]);
        } while (!n->is_leaf);

        return n;
    }

    // Method to insert a key (id) in the B+ tree (to the index_file)
    //void insert(int id) {
    //}

    // Insert new record into the data_file and relevant information to the index_file
    void insertRecord(Record record) {
        if (root == nullptr) {
            // creating very first record
            // I'll start by creating root node and a first leaf node, where the addresses will live.
            // Creating leaf node
            BPlusNode* n = new BPlusNode();
            n->is_leaf = true;
            n->localNodeId = globalNodeId;
            globalNodeId++;

            n->keys.push_back(record.id);
            n->addresses.push_back(0);

            //Creating root node
            root = new BPlusNode();
            root->is_leaf = false;
            root->localNodeId = globalNodeId;
            globalNodeId++;

            //root->children.push_back(n);
            root->addresses.push_back(n->localNodeId);
            root->keys.push_back(record.id);
            //root->parent = nullptr;
            root->parentId = MAGIC_INT_EMPTY;
            //n->parent = root;
            n->parentId = root->localNodeId;

            saveToDisk(n);
        }
        else if (root->addresses.size() == 1) {
            // we have only one node leaf node under root
            BPlusNode* cacheN = getNodeFromDisk(root->addresses[0]);
            insertPointer(cacheN, record.id, myRecordNum);
        }
        else {
            // searching for a suitable spot and inserting pointer
            BPlusNode* n = root;
            do {
                int p = 0;
                while (p < n->keys.size() - 1 && record.id > n->keys[p + 1])
                {
                    p++;
                }
                n = getNodeFromDisk(n->addresses[p]);
            } while (!n->is_leaf);

            insertPointer(n, record.id, myRecordNum);
        }
        manager->insertRecord(record, myRecordNum);
        myRecordNum++;
    }

    // method to insert the child into non-leaf node.
    void insertChild(BPlusNode* thisNode, int newNodeId, int newNodeKey) {
        if (!thisNode->is_leaf) {
            int p = 0;
            while (p < thisNode->keys.size() && newNodeKey > thisNode->keys[p]) {
                p++;
            }
            thisNode->keys.insert(thisNode->keys.begin() + p, 1, newNodeKey);
            thisNode->addresses.insert(thisNode->addresses.begin() + p, 1, newNodeId);     // this field to store local ids

            // handling the overflow situation
            if (thisNode->keys.size() > NODE_DEGREE * 2) {
                // constructing new node and save it after we finish processing...
                BPlusNode* n = new BPlusNode();
                n->is_leaf = false;
                n->localNodeId = globalNodeId;
                globalNodeId++;
                // ****** //

                for (int i = NODE_DEGREE; i < thisNode->keys.size(); i++) {
                    n->keys.push_back(thisNode->keys[i]);
                    // don't forget to change these child's parent!!
                    BPlusNode* child = getNodeFromDisk(thisNode->addresses[i]);
                    child->parentId = n->localNodeId;
                    saveToDisk(child);
                    //n->children.push_back(thisNode->children[i]);
                    n->addresses.push_back(thisNode->addresses[i]);
                }
                thisNode->keys.resize(NODE_DEGREE);
                //thisNode->children.resize(NODE_DEGREE);
                thisNode->addresses.resize(NODE_DEGREE);

                if (thisNode->parentId == MAGIC_INT_EMPTY) {
                    // this is where the root node gets splitted.

                    // One more constructing node, saving after processing...
                    BPlusNode* r = new BPlusNode();
                    r->is_leaf = false;
                    r->localNodeId = globalNodeId;
                    globalNodeId++;
                    // ****** //

                    r->keys.push_back(thisNode->keys[0]);
                    //r->children.push_back(thisNode);
                    r->addresses.push_back(thisNode->localNodeId);
                    thisNode->parentId = r->localNodeId;

                    r->keys.push_back(n->keys[0]);
                    //r->children.push_back(n);
                    r->addresses.push_back(n->localNodeId);
                    n->parentId = r->localNodeId;

                    // changing tree root
                    this->root = r;
                }
                else {
                    n->parent = thisNode->parent; // <- check if this line is needed!
                    insertChild(thisNode->parent, n->localNodeId, n->keys[0]);
                }
                saveToDisk(n);
            }
            saveToDisk(thisNode);
        }
        else {
            // Do nothing (debug)
        }
    }

    // method to insert the key + address pair into leaf node.
    void insertPointer(BPlusNode* thisNode, int key, int pos) {
        if (thisNode->is_leaf) {
            // Inserting only to leaf nodes
            int p = 0;
            while (p < thisNode->keys.size() && key > thisNode->keys[p]) {
                p++;
            }
            thisNode->keys.insert(thisNode->keys.begin() + p, 1, key);
            thisNode->addresses.insert(thisNode->addresses.begin() + p, 1, pos);

            // updating parent's index if inserted into p = 0
            if (p == 0) {
                if (thisNode->parentId == root->localNodeId) {
                    // updating in root node (in memory)
                    for (int i = 0; i < root->addresses.size(); i++) {
                        if (thisNode->localNodeId == root->addresses[i]) {
                            root->keys[i] = thisNode->keys[0];
                            break;
                        }
                    }
                }
                else {
                    // updating parent node stored on disk
                    BPlusNode* parent = getNodeFromDisk(thisNode->parentId);
                    for (int i = 0; i < parent->addresses.size(); i++) {
                        if (thisNode->localNodeId == parent->addresses[i]) {
                            parent->keys[i] = thisNode->keys[0];
                            break;
                        }
                    }
                    saveToDisk(parent);
                }
            }

            // handling the overflow situation
            if (thisNode->keys.size() > NODE_DEGREE * 2) {
                // constructing new node and save it after we finish processing...
                BPlusNode* n = new BPlusNode();
                n->is_leaf = true;
                n->localNodeId = globalNodeId;
                globalNodeId++;
                // ****** //

                for (int i = NODE_DEGREE; i < thisNode->keys.size(); i++) {
                    n->keys.push_back(thisNode->keys[i]);
                    n->addresses.push_back(thisNode->addresses[i]);
                }
                thisNode->keys.resize(NODE_DEGREE);
                thisNode->addresses.resize(NODE_DEGREE);

                // Debug everything, step 1
                BPlusNode* parent;
                if (thisNode->parentId == root->localNodeId) {
                    parent = root;
                }
                else {
                    parent = getNodeFromDisk(thisNode->parentId);
                }
                n->parentId = parent->localNodeId;
                saveToDisk(n);

                insertChild(parent, n->localNodeId, n->keys[0]);
                //saveToDisk(parent);
                //delete(n);
            }
            saveToDisk(thisNode);
        }
        else {
            // Do nothing (debug)
        }
    }

    // Read csv file and add records to the index_file and data_file
    void createFromFile(string csvFName) {
        ifstream in_file;

        in_file.open(csvFName, ios::in);

        string line, field;
        vector<string> fields;

        // grab entire line
        while (getline(in_file, line, '\n'))
        {
            // turn line into a stream
            stringstream s(line);

            // gets everything in stream up to comma
            fields.clear();
            getline(s, field, ',');
            fields.push_back(field);
            getline(s, field, ',');
            fields.push_back(field);
            getline(s, field, ',');
            fields.push_back(field);
            getline(s, field, ',');
            fields.push_back(field);

            Record r = Record(fields);
            insertRecord(r);
        }
        in_file.close();
        manager->saveCache();
        manager->TotalRecords = myRecordNum;
    }

    // Given an id, find the relevant record and print it
    Record findRecordById(int id) {
        // Use BPlusNode* search(int id) to find the index first
        BPlusNode* n = search(id);
        for (int i = 0; i < n->keys.size(); i++) {
            if (n->keys[i] == id) {
                return manager->findRecordById(id, n->addresses[i]);
            }
        }

        // Didn't find :(
        return Record(id, "Not", "Found", 0);
    };

    // Saves node to disk
    void saveToDisk(BPlusNode* node) {
        fstream indexStream;
        indexStream.open(INDEX_FILE_NAME, ios::binary | ios::in | ios::out);
        char recordCast[BPLUSTREE_NODE_SIZE];
        char* t = recordCast;

        // local id
        *reinterpret_cast<int*>(t) = node->localNodeId;
        t += sizeof(int);

        // keys
        for (int i = 0; i < 2 * NODE_DEGREE; i++) {
            if (node->keys.size() > i) {
                *reinterpret_cast<int*>(t) = node->keys[i];
            }
            else {
                *reinterpret_cast<int*>(t) = MAGIC_INT_EMPTY;
            }
            t += sizeof(int);
        }

        // childs (legacy)
        t += sizeof(int) * 2 * NODE_DEGREE;

        // is leaf
        *reinterpret_cast<int*>(t) = (int)node->is_leaf;
        t += sizeof(int);

        // parent (legacy)
        t += sizeof(int);

        // parent id
        if (node->parentId != MAGIC_INT_EMPTY) {
            *reinterpret_cast<int*>(t) = node->parentId;
        }
        t += sizeof(int);

        // addresses
        for (int i = 0; i < 2 * NODE_DEGREE; i++) {
            if (node->addresses.size() > i) {
                *reinterpret_cast<int*>(t) = node->addresses[i];
            }
            else {
                *reinterpret_cast<int*>(t) = MAGIC_INT_EMPTY;
            }
            t += sizeof(int);
        }

        // saving the localId of the root node into the first int
        char rootCast[sizeof(int)];
        char* tRoot = rootCast;
        *reinterpret_cast<int*>(tRoot) = root->localNodeId;
        indexStream.seekp(0, ios_base::beg);
        indexStream.write(rootCast, sizeof(int));

        streamoff offset = sizeof(int) + node->localNodeId * BPLUSTREE_NODE_SIZE;
        indexStream.seekp(offset, ios_base::beg);
        indexStream.write(recordCast, BPLUSTREE_NODE_SIZE);

        indexStream.close();
    }

    // Fetches the node from disk by its local Id.
    BPlusNode* getNodeFromDisk(int localId) {
        ifstream ifs;
        ifs.open(INDEX_FILE_NAME, ios::binary);

        // read node
        streamoff offset = sizeof(int) + localId * BPLUSTREE_NODE_SIZE;
        ifs.seekg(offset, ios_base::beg);
        char* addr = new char[BPLUSTREE_NODE_SIZE];

        BPlusNode* node = new BPlusNode;

        // local id
        ifs.read(addr, sizeof(int));
        node->localNodeId = *reinterpret_cast<int*>(addr);
        addr += sizeof(int);

        // keys
        for (int i = 0; i < 2 * NODE_DEGREE; i++) {
            ifs.read(addr, sizeof(int));
            int key = *reinterpret_cast<int*>(addr);
            if (key != MAGIC_INT_EMPTY) {
                node->keys.push_back(key);
            }
            addr += sizeof(int);
        }

        // childs (legacy)
        ifs.read(addr, sizeof(int) * 2 * NODE_DEGREE);
        addr += sizeof(int)* 2 * NODE_DEGREE;

        // is leaf
        ifs.read(addr, sizeof(int));
        node->is_leaf = (bool)*reinterpret_cast<int*>(addr);
        addr += sizeof(int);

        // parent (legacy)
        ifs.read(addr, sizeof(int));
        addr += sizeof(int);

        // parent id
        ifs.read(addr, sizeof(int));
        int parent_id = *reinterpret_cast<int*>(addr);
        node->parentId = parent_id;
        addr += sizeof(int);

        // addresses
        for (int i = 0; i < 2 * NODE_DEGREE; i++) {
            ifs.read(addr, sizeof(int));
            int bbb = *reinterpret_cast<int*>(addr);
            if (bbb != MAGIC_INT_EMPTY) {
                node->addresses.push_back(bbb);
            }
            addr += sizeof(int);
        }

        ifs.close();
        return node;
    };
};
