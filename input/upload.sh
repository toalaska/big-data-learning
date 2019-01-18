cd /mnt/c/Users/Myra/projects/hadoop/input

hadoop fs -rm /input/mobiles.txt
hadoop fs -put mobiles.txt /input/
hadoop fs -put recorder.txt /input/
hadoop fs -put dept.txt /input/
hadoop fs -put emp.txt /input/
hadoop fs -ls /input/
