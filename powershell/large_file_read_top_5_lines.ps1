$filename = "C:\large_file.txt"
$file = new-object System.IO.StreamReader($filename)
$file.ReadLine()
$file.ReadLine()
$file.ReadLine()
$file.ReadLine()
$file.ReadLine()
$file.close()