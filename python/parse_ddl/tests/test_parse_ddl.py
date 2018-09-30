# -*- coding: utf-8 -*-
"""
Test parse_ddl functions

@author: jong
"""


import unittest
import json
from .context import parse_ddl

class TestParseSQL(unittest.TestCase):

    def test_remove_empty_lists(self):
        lista = [1, 2, 3]
        listb = ["a", "b", "c"]
        listc = []
        lisotype = []
        list2 = []
        lisotype.append(lista)
        lisotype.append(listb)
        lisotype.append(listc)
        lisotype = parse_ddl.remove_empty_lists(lisotype)
        list2.append(lista)
        list2.append(listb)
        self.assertEqual(lisotype, list2)


    def test_get_file_content(self):
        file1 = "tests\\file1.sql"  # UCS2 LE BOM
        file2 = "tests\\file2.sql"  # UTF-8
        file3 = "tests\\file3.sql"  # ANSI
        contenotype = parse_ddl.get_file_content(file1)
        content2 = parse_ddl.get_file_content(file2)
        content3 = parse_ddl.get_file_content(file3)
        self.assertEqual(contenotype, content2)
        self.assertEqual(contenotype, content3)


    def test_find_ddls_table(self):
        otype = "table"
        regen_assertions = False  # Always false unless examples change
        file_path = "tests\\ddl_examples\\ddl_%s_examples.sql" % (otype)
        assertions = "tests\\ddl_examples\\ddl_%s_assertions.json" % (otype)
        file_content = parse_ddl.get_file_content(file_path)
        ddl = parse_ddl.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as file:
            assert_data = file.read()
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list, ddl)


    def test_find_ddls_procedure(self):
        otype = "procedure"
        regen_assertions = False  # Always false unless examples change
        file_path = "tests\\ddl_examples\\ddl_%s_examples.sql" % (otype)
        assertions = "tests\\ddl_examples\\ddl_%s_assertions.json" % (otype)
        file_content = parse_ddl.get_file_content(file_path)
        ddl = parse_ddl.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as file:
            assert_data = file.read()
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list, ddl)


    def test_find_ddls_view(self):
        otype = "view"
        regen_assertions = False  # Always false unless examples change
        file_path = "tests\\ddl_examples\\ddl_%s_examples.sql" % (otype)
        assertions = "tests\\ddl_examples\\ddl_%s_assertions.json" % (otype)
        file_content = parse_ddl.get_file_content(file_path)
        ddl = parse_ddl.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as file:
            assert_data = file.read()
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list, ddl)


    def test_find_ddls_function(self):
        otype = "function"
        regen_assertions = False  # Always false unless examples change
        file_path = "tests\\ddl_examples\\ddl_%s_examples.sql" % (otype)
        assertions = "tests\\ddl_examples\\ddl_%s_assertions.json" % (otype)
        file_content = parse_ddl.get_file_content(file_path)
        ddl = parse_ddl.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as file:
            assert_data = file.read()
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list, ddl)


    def test_find_ddls_type(self):
        otype = "type"
        regen_assertions = False  # Always false unless examples change
        file_path = "tests\\ddl_examples\\ddl_%s_examples.sql" % (otype)
        assertions = "tests\\ddl_examples\\ddl_%s_assertions.json" % (otype)
        file_content = parse_ddl.get_file_content(file_path)
        ddl = parse_ddl.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as file:
            assert_data = file.read()
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list, ddl)



    def test_find_ddls_trigger(self):
        otype = "trigger"
        regen_assertions = False  # Always false unless examples change
        file_path = "tests\\ddl_examples\\ddl_%s_examples.sql" % (otype)
        assertions = "tests\\ddl_examples\\ddl_%s_assertions.json" % (otype)
        file_content = parse_ddl.get_file_content(file_path)
        ddl = parse_ddl.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as file:
            assert_data = file.read()
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list, ddl)


    def test_find_ddls_index(self):
        otype = "index"
        regen_assertions = False  # Always false unless examples change
        file_path = "tests\\ddl_examples\\ddl_%s_examples.sql" % (otype)
        assertions = "tests\\ddl_examples\\ddl_%s_assertions.json" % (otype)
        file_content = parse_ddl.get_file_content(file_path)
        ddl = parse_ddl.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as file:
            assert_data = file.read()
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list, ddl)


    def test_find_ddls_rename(self):
        otype = "rename"
        regen_assertions = False  # Always false unless examples change
        file_path = "tests\\ddl_examples\\ddl_%s_examples.sql" % (otype)
        assertions = "tests\\ddl_examples\\ddl_%s_assertions.json" % (otype)
        file_content = parse_ddl.get_file_content(file_path)
        ddl = parse_ddl.find_ddls(file_content)
        # This is to regenerate the json assertion if examples change
        if regen_assertions:
            with open(assertions, "w") as outfile:
                json.dump(ddl, outfile)
        # Read the ddl assertions json into a list for comparing
        with open(assertions, 'r') as file:
            assert_data = file.read()
        assert_list = json.loads(assert_data)
        self.assertEqual(assert_list, ddl)


    def test_parse_ddl_to_dataframe(self):
        the_directory = "tests\\ddl_examples"
        the_df = parse_ddl.parse_ddl_to_dataframe(the_directory)
        self.assertEqual((8, 7), the_df.shape)


    def test_split_name_schema(self):
        name_schema = "[prod].[mytable]"
        name_schema = parse_ddl.split_name_schema(name_schema)
        self.assertEqual(name_schema[0], "prod")
        self.assertEqual(name_schema[1], "mytable")

        name_schema = "mydbname.dbo.ProcedureName"
        name_schema = parse_ddl.split_name_schema(name_schema)
        self.assertEqual(name_schema[0], "dbo")
        self.assertEqual(name_schema[1], "ProcedureName")


    def test_ddl_object_info(self):
        the_string = "ALTER TABLE [dbo].[mytablea] (a1 int NOT NULL)"
        assert_info = ("ALTER",
                       "TABLE",
                       "dbo",
                       "mytablea")
        obj_info = parse_ddl.ddl_object_info(the_string)
        self.assertEqual(assert_info, obj_info)

        the_string = "Create Procedure [dbo].[spSSS_Update]"
        assert_info = ("CREATE",
                       "PROCEDURE",
                       "dbo",
                       "spSSS_Update")
        obj_info = parse_ddl.ddl_object_info(the_string)
        self.assertEqual(assert_info, obj_info)

        the_string = "drop proc spSSS_Update"
        assert_info = ("DROP",
                       "PROCEDURE",
                       "dbo",
                       "spSSS_Update")
        obj_info = parse_ddl.ddl_object_info(the_string)
        self.assertEqual(assert_info, obj_info)

        the_string = "drop table [dbo].[c_Aaaa_Bbb]"
        assert_info = ("DROP",
                       "TABLE",
                       "dbo",
                       "c_Aaaa_Bbb")
        obj_info = parse_ddl.ddl_object_info(the_string)
        self.assertEqual(assert_info, obj_info)

        the_string = "into BK_GOTCH_234_CheckIDFix_ON_PART from"
        assert_info = ("CREATE",
                       "TABLE",
                       "dbo",
                       "BK_GOTCH_234_CheckIDFix_ON_PART")
        obj_info = parse_ddl.ddl_object_info(the_string)
        self.assertEqual(assert_info, obj_info)

        the_string = ("sp_rename \
		@objname = N'dbo.CentralLogging.AppicationName', \
		@newname = 'ApplicationName'")
        assert_info = ("SP_RENAME",
                       None,
                       "dbo",
                       "ApplicationName")
        obj_info = parse_ddl.ddl_object_info(the_string)
        self.assertEqual(assert_info, obj_info)
        
        the_string = ("sp_rename \
                      N'dbo.Audits.IX_Audits_AuditSubCategoryID', \
                      N'IX_Audits_AuditSubCategoryID_old'")
        assert_info = ("SP_RENAME",
                       None,
                       "dbo",
                       "IX_Audits_AuditSubCategoryID_old")
        obj_info = parse_ddl.ddl_object_info(the_string)
        self.assertEqual(assert_info, obj_info)        

if __name__ == '__main__':
    unittest.main()
