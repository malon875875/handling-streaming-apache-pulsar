package com.pluralsight.functions;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RoutingFunction implements Function<String, Void> {

    @Override
    public Void process(String input, Context context) throws Exception {
        Logger LOG = context.getLogger();
        LOG.info(String.format("Got this input: %s", input));
		Price inputPrice  = new Price(input);
	    String topic = String.format("year-%s", inputPrice.getYear());
	    context.newOutputMessage(topic, Schema.STRING).value(inputPrice.getPrice()).send();
        return null;

    }

    public static void main(String[] args) throws Exception {

    	// functionConfig is the same as:
		// 		bin/pulsar-admin functions create --jar target/functions-0.2.0.jar --classname com.pluralsight.functions.RoutingFunction --name routing-debug --inputs "voo" --log-topic logging-function-logs

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setName("routing-debug");
        functionConfig.setInputs(Collections.singleton("voo"));
        functionConfig.setClassName(RoutingFunction.class.getName());
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
	    functionConfig.setLogTopic("logging-function-logs");

	    //Allow local Firewall rule to localhost 127.0.0.1:6650
	    functionConfig.setJar("target/functions-0.2.0.jar");

	    LocalRunner localRunner = LocalRunner.builder().functionConfig(functionConfig).build();
        localRunner.start(false);

        // 	mvn package
		//	Set a breakpoint after context.newOutputMessage()
		//  run this main() in Debug mode
        //  within the docker instance of pulsar run:
		// 		bin/pulsar-client produce voo --messages "2011-09-16,180.197" -s ":"
    }

}

class Price {
	private Date date;

	int getYear() {
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);
		return year;
	}

	String getPrice() {
		return String.valueOf(price);
	}

	private float price;

	Price(String priceString) throws ParseException {
		String[] inputList = priceString.split(",");
		date = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(inputList[0]);
		price = Float.parseFloat(inputList[1]);
	}
}
