declare @temptable table (columna nvarchar(50))

insert into @temptable (columna)
EXEC sp_execute_external_script @language = N'Python', 
@script = N'
import pandas as pd
# lets create a list of songs.
songs = ["In the name of love","Scream","Till the sky falls down","In and out of Love"]
# lets also create a list of corresponding artists. FYI: "MG" stands # for Martin Garrix, "TI" for Tiesto, "DB" for Dash Berlin, "AV"for # Armin Van Buuren.
artists = ["MG","TI","DB","AV"]
# likewise lets create a dictionary that contains artists and songs.
song_arts = {"MG":"In the name of love","TI":"Scream","DB":"Till the sky falls down","AV":"In and out of Love"}

# create a Series object whose data is coming from songs list.
ser_num = pd.Series(data=songs)
ser_num
print (ser_num)

ser_dict= pd.Series(song_arts)
MyOutput = pd.DataFrame.from_dict(ser_num)
print (ser_dict)
',
@output_data_1_name = N'MyOutput'

select * from @temptable