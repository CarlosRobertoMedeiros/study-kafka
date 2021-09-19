package br.com.roberto.ecommerce.consumer;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import br.com.roberto.ecommerce.Message;
import br.com.roberto.ecommerce.MessageAdapter;

public class GsonDeserializer<T> implements Deserializer<Message<T>> {

	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

	@Override
	public Message deserialize(String s, byte[] bytes) {
		return gson.fromJson(new String(bytes), Message.class);
	}

}
