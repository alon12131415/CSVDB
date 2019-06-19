import csv

csv_text = ['",,,,,""""\na",b,c', 'd,e,f']
for row in csv.reader(csv_text):
	print(row)
