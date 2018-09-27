# -*- coding: utf-8 -*-
"""
Test parse_sql functions

@author: jong
"""


import unittest
import parse_sql
import json

class TestParseSQL(unittest.TestCase):

    def test_remove_empty_lists(self):
        lista = [1,2,3]
        listb = ["a","b","c"]
        listc = []
        list1 = []
        list2 = []
        list1.append(lista)
        list1.append(listb)
        list1.append(listc)
        list1 = parse_sql.remove_empty_lists(list1)
        list2.append(lista)
        list2.append(listb)
        self.assertEqual(list1, list2)
    
    
    def test_get_file_content(self):
        file1 = "test\\file1.sql"  # UCS2 LE BOM
        file2 = "test\\file2.sql"  # UTF-8
        file3 = "test\\file3.sql"  # ANSI
        content1 = parse_sql.get_file_content(file1)
        content2 = parse_sql.get_file_content(file2)
        content3 = parse_sql.get_file_content(file3)
        self.assertEqual(content1, content2)
        self.assertEqual(content1, content3)


    def test_find_ddls_table(self):
        t1 = "table"
        regen_assertions = False  # Always false unless examples change
        file_path = "test\\ddl_examples\\ddl_%s_examples.sql" % (t1)
        assertions = "test\\ddl_examples\\ddl_%s_assertions.json" % (t1)
        file_content = parse_sql.get_file_content(file_path)
        ddl = parse_sql.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as f:
            assert_data = f.read()  
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list,ddl)


    def test_find_ddls_procedure(self):
        t1 = "procedure"
        regen_assertions = False  # Always false unless examples change
        file_path = "test\\ddl_examples\\ddl_%s_examples.sql" % (t1)
        assertions = "test\\ddl_examples\\ddl_%s_assertions.json" % (t1)
        file_content = parse_sql.get_file_content(file_path)
        ddl = parse_sql.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as f:
            assert_data = f.read()  
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list,ddl)


    def test_find_ddls_view(self):
        t1 = "view"
        regen_assertions = False  # Always false unless examples change
        file_path = "test\\ddl_examples\\ddl_%s_examples.sql" % (t1)
        assertions = "test\\ddl_examples\\ddl_%s_assertions.json" % (t1)
        file_content = parse_sql.get_file_content(file_path)
        ddl = parse_sql.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as f:
            assert_data = f.read()  
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list,ddl)


    def test_find_ddls_function(self):
        t1 = "function"
        regen_assertions = False  # Always false unless examples change
        file_path = "test\\ddl_examples\\ddl_%s_examples.sql" % (t1)
        assertions = "test\\ddl_examples\\ddl_%s_assertions.json" % (t1)
        file_content = parse_sql.get_file_content(file_path)
        ddl = parse_sql.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as f:
            assert_data = f.read()  
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list,ddl)


    def test_find_ddls_type(self):
        t1 = "type"
        regen_assertions = False  # Always false unless examples change
        file_path = "test\\ddl_examples\\ddl_%s_examples.sql" % (t1)
        assertions = "test\\ddl_examples\\ddl_%s_assertions.json" % (t1)
        file_content = parse_sql.get_file_content(file_path)
        ddl = parse_sql.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as f:
            assert_data = f.read()  
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list,ddl)



    def test_find_ddls_trigger(self):
        t1 = "trigger"
        regen_assertions = False  # Always false unless examples change
        file_path = "test\\ddl_examples\\ddl_%s_examples.sql" % (t1)
        assertions = "test\\ddl_examples\\ddl_%s_assertions.json" % (t1)
        file_content = parse_sql.get_file_content(file_path)
        ddl = parse_sql.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as f:
            assert_data = f.read()  
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list,ddl)
        
        
    def test_find_ddls_index(self):
        t1 = "index"
        regen_assertions = False  # Always false unless examples change
        file_path = "test\\ddl_examples\\ddl_%s_examples.sql" % (t1)
        assertions = "test\\ddl_examples\\ddl_%s_assertions.json" % (t1)
        file_content = parse_sql.get_file_content(file_path)
        ddl = parse_sql.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as f:
            assert_data = f.read()  
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list,ddl)


    def test_find_ddls_rename(self):
        t1 = "rename"
        regen_assertions = False  # Always false unless examples change
        file_path = "test\\ddl_examples\\ddl_%s_examples.sql" % (t1)
        assertions = "test\\ddl_examples\\ddl_%s_assertions.json" % (t1)
        file_content = parse_sql.get_file_content(file_path)
        ddl = parse_sql.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as f:
            assert_data = f.read()  
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list,ddl)


    def test_parse_sql_to_dataframe(self):
        the_directory = "test\\ddl_examples"
        the_df = parse_sql.parse_sql_to_dataframe(the_directory)
        #self.assertEqual(content1, content2)
        #self.assertEqual(content1, content3)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestParseSQL)
    unittest.TextTestRunner(verbosity=2).run(suite)