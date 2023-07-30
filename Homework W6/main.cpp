// Evgeny Ovechnikov
// Course: CS540 - Database Management Systems
// Homework: Hash Indexing (w5)

/*
Skeleton code for linear hash indexing
*/
#include <string>
#include <ios>
#include <fstream>
#include <vector>
#include <string>
#include <string.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cmath>
#include "classes.h"
using namespace std;


int main(int argc, char* const argv[]) {

    // Create the index
    LinearHashIndex emp_index("EmployeeIndex");
    emp_index.createFromFile("Employee.csv");
    
    // Loop to lookup IDs until user is ready to quit
    while (true)
    {
        // Get input from command line
        string ids_text;
        int id_int = 0;

        cout << "Enter id:";
        getline(std::cin, ids_text);

        istringstream ids_text_stream(ids_text);
        string id_text;

        while (getline(ids_text_stream, id_text, ' ')) {
            try {
                id_int = stoi(id_text);
            }
            catch (const std::exception& e) {
                std::cerr << "Invalid input \n";
                continue;
            }

            // Lookup
            Record r = emp_index.findRecordById(id_int);
            r.print();
        }
    }

    return 0;
}
