package org.gbif.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class NameUsage implements Serializable {
  public int key;
  public Integer parentKey;
  public boolean hidden;
  public String rank;
  public String name;
  public String comments;
  public List<String> synonyms = new ArrayList<>();
  public List<String> vernacular = new ArrayList<>();

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof NameUsage)) return false;
    NameUsage nameUsage = (NameUsage) o;
    return key == nameUsage.key &&
        parentKey == nameUsage.parentKey &&
        hidden == nameUsage.hidden &&
        Objects.equals(rank, nameUsage.rank) &&
        Objects.equals(name, nameUsage.name) &&
        Objects.equals(comments, nameUsage.comments) &&
        Objects.equals(synonyms, nameUsage.synonyms) &&
        Objects.equals(vernacular, nameUsage.vernacular);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, parentKey, hidden, rank, name, comments, synonyms, vernacular);
  }
}
