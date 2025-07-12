# kafka-connect-stuct-to-decimal-convertor

### This plugin convert type of field from string to timestamp

[Download Plugin](./plugin)


```
# For example: 

From this:
			"total_amount": {
				"scale": 0,
				"value": "BRY="
			},

to this: total_amount : 655 in postgres 

# Let's get start

git clone https://github.com/nicat-m/kconnect-stuct-to-decimal-convertor.git

mvn clean install

# copy jar file under target folder to kafka connect plugin folder

cp -pr target/connect-transform-decimal-1.0.0.jar /opt/kafka/plugins/

# edit kafka-connect properties

vim /opt/kafka/config/connect-distributed.properties

# set this path like this

plugin.path=/opt/kafka/plugins

# then restart your kafka connect

# sink connector config:

  "transforms": "decimalConvert"
  "transforms.decimalConvert.type": "org.example.StructToDecimal",
  "transforms.decimalConvert.field.name": "total_amount",
  
```