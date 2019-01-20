cd /mnt/c/Users/Myra/projects/hadoop/input

hadoop fs -rm /input/mobiles.txt
hadoop fs -put mobiles.txt /input/
hadoop fs -put recorder.txt /input/
hadoop fs -put dept.txt /input/
hadoop fs -put emp.txt /input/
hadoop fs -put nums.txt /input/
hadoop fs -rm  /input/traffice.txt
hadoop fs -put traffice.txt /input/
hadoop fs -ls /input/
