package chronicle.db.service;

import java.util.List;

import chronicle.db.entity.IChronicle;
import net.openhft.chronicle.bytes.BytesMarshallable;

public class Lead implements BytesMarshallable, IChronicle {
    public String fullName, linkedin, facebook, twitter, email, mobilePhone, jobTitle, location;
    public List<Email> emails;

    public Lead() {
    }

    public Lead(final String fullName, final String linkedin, final String facebook, final String twitter,
            final String email, final String mobilePhone, final String jobTitle, final String location,
            final List<Email> emails) {
        this.fullName = fullName;
        this.linkedin = linkedin;
        this.facebook = facebook;
        this.twitter = twitter;
        this.email = email;
        this.mobilePhone = mobilePhone;
        this.jobTitle = jobTitle;
        this.location = location;
        this.emails = emails;
    }

    @Override
    public String toString() {
        return "Lead [fullName=" + fullName + ", linkedin=" + linkedin + ", facebook=" + facebook + ", twitter="
                + twitter + ", email=" + email + ", mobilePhone=" + mobilePhone + ", jobTitle=" + jobTitle
                + ", location=" + location + ", emails=" + emails + "]";
    }

    @Override
    public Object[] row(final String key) {
        return new Object[] { key, fullName, linkedin, facebook, twitter, email, mobilePhone, jobTitle, location,
                emails.toString() };
    }

    @Override
    public String[] header() {
        return new String[] { "ID", "Full Name", "LinkedIn", "Facebook", "Twitter", "Email", "Mobile Phone",
                "Job Title", "Location", "Emails" };
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        return switch (fieldName) {
            case "fullName" -> fullName;
            case "linkedin" -> linkedin;
            case "facebook" -> facebook;
            case "twitter" -> twitter;
            case "email" -> email;
            case "mobilePhone" -> mobilePhone;
            case "jobTitle" -> jobTitle;
            case "location" -> location;
            case "emails" -> emails != null ? List.copyOf(emails) : null;
            default -> throw new IllegalArgumentException(
                    "Unknown field: " + fieldName + " for object: " + this.getClass().getSimpleName());
        };
    }

}
