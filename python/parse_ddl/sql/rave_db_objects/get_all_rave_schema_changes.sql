SELECT gctt.git_tag1
	  ,gpdo.git_tag
	  ,gtd.git_tag_date
	  ,gpdo.object_type
	  ,gpdo.object_schema
	  ,gpdo.object_name
	  ,gctt.dir_path
	  ,gctt.file_name
	  ,LTRIM(RTRIM(REPLACE(REPLACE(gpdo.ddl, CHAR(10),' '), CHAR(13),' '))) as ddl
FROM parse_ddl.dbo.git_compare_two_tags gctt 
INNER JOIN parse_ddl.dbo.git_parse_ddl_objects gpdo on gctt.full_path = gpdo.full_path and gctt.git_repo = gpdo.git_repo and gctt.git_tag2 = gpdo.git_tag
INNER JOIN parse_ddl.dbo.git_tag_dates gtd on gctt.git_repo = gtd.git_repo and gctt.git_tag2 = gtd.git_tag
WHERE gctt.git_tag1 IS NOT NULL
AND gctt.git_repo = 'Rave'
AND gpdo.object_type = 'TABLE'
AND gpdo.object_name <> ''
AND gctt.dir_path NOT LIKE '%GoldBackup%'
AND gctt.dir_path LIKE '%change%'
--AND gctt.dir_path LIKE '%Rave\Medidata 5 RAVE Database Project\Rave_Viper_Lucy_Merged_DB_Scripts\DailyChanges'
AND gpdo.object_name IS NOT NULL
AND gpdo.ddl NOT LIKE '%ADD%CONSTRAINT%'
AND gpdo.object_name NOT LIKE 'BK_%'
--AND gpdo.object_name IN ('ActionTypeR','Activations','ActivationStatusR','AnalyteRanges','Analytes','AuditActionRestrictions','AuditCategoryR','Audits','AuditSubCategoryR','ChangeCodeRoles','ChangeCodes','Checks','ClinicalSignificance','ClinicalSignificanceCodes','CoderDecisions','CoderValues','CodingColumns','CodingDictionaries','CodingValues','Configuration','CRFVersions','CustomFunctions','DataDictionaries','DataDictionaryEntries','DataPages','DataPoints','DataPointUntranslated','Derivations','eLearningCourses','eLearningCourseStudyRoles','eLearningUserCourses','ExternalUsers','FieldRestrictions','FieldReviewGroups','Fields','FolderForms','Folders','FormRestrictions','Forms','IdentifierTypeR','ImpliedActionTypes','Instances','LabAssignments','Labs','LabStandardGroupEntries','LabStandardGroups','LabUnitConversions','LabUnitDictionaries','LabUnitDictionaryEntries','LabUnits','Localizations','LocalizedDataStrings','LocalizedStrings','LockStatusR','LogicalRecordPositionR','MarkingGroupCategoryR','MarkingGroups','Markings','MarkingTypeR','Matrix','MatrixActionR','MigrationPlan','MigrationRun','MigrationSubjectList','ObjectTagQualifiers','ObjectTags2','ObjectTypeR','Projects','ProjectSourceSystemR','ProtocolDeviationClassR','ProtocolDeviationCodeR','QueryStatusR','RangeTypes','RangeTypeVariables','RavePatches','Records','ReviewGroups','ReviewStatusR','RoleActions','RolePermissionR','RolesAllModules','RoleSubjectStatusAccess','RSGMap_Configurations','RSGMap_DictionaryItemMappings','RSGMap_TagsR','RSGMap_FormMappings','RSGMap_FieldMappings','RSGMap_StudyConfigurations','SecurityGroup','SecurityGroupUser','SharedSubjects','SharingRelationships','Signatures','SiteGroups','Sites','Studies','StudySiteInvestigators','StudySites','SubjectMatrix','Subjects','SubjectStatus','SubjectStatusCategoryR','SubjectStatusHistory','Timezones','UnitDictionaries','UnitDictionaryEntries','UserGroups','UserModules','UserObjectRole','UserPermissionHistory','UserPermissionTypeR','Users','UserSettings','UserStudySites','Variables')
ORDER BY gtd.git_tag_date, gpdo.object_schema, gpdo.object_name
