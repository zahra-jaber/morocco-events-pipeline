package com.morocco.events.deserializer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.morocco.events.model.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class EventDeserializer implements DeserializationSchema<Event> {
    private static final long serialVersionUID = 1L;
    private transient Gson gson;

    @Override
    public void open(InitializationContext context) throws Exception {
        this.gson = new Gson();
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        try {
            String jsonString = new String(message);
            Event event = gson.fromJson(jsonString, Event.class);
            // Si timestamp est 0, utiliser le temps actuel
            if (event.getTimestamp() == 0) {
                event.setTimestamp(System.currentTimeMillis());
            }
            return event;
        } catch (JsonSyntaxException e) {
            throw new IOException("Failed to deserialize JSON message", e);
        }
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
