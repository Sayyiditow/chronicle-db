package chronicle.db;

import net.openhft.chronicle.bytes.BytesMarshallable;

public class Email implements BytesMarshallable {
    public String address, type;

    public Email(final String address, final String type) {
        this.address = address;
        this.type = type;
    }

    public Email() {
    }

    @Override
    public String toString() {
        return "Email [address=" + address + ", type=" + type + "]";
    }

}
