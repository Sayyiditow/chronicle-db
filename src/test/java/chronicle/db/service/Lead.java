package chronicle.db.service;

import java.util.List;

import chronicle.db.entity.ICsv;
import net.openhft.chronicle.bytes.BytesMarshallable;

public class Lead implements BytesMarshallable, ICsv<String> {
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

}
