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
  public List<TypeMaterial> typeMaterial = new ArrayList<>();
  public List<Citation> citations = new ArrayList<>();

  public static class Citation implements Serializable {
    public String citation;
    public String medlineID;
    public String pubmedID;
    public String url;

    public String identifier() {
      if (url != null) {
        return url;
      }
      if (pubmedID != null) {
        return "pubmed:" + pubmedID;
      }
      if (medlineID != null) {
        return "medline:" + medlineID;
      }
      return null;
    }
  }

  public static class TypeMaterial implements Serializable {
    public String citation;
    public String status;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TypeMaterial)) return false;
      TypeMaterial that = (TypeMaterial) o;
      return Objects.equals(citation, that.citation) &&
          Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
      return Objects.hash(citation, status);
    }
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof NameUsage)) return false;
    NameUsage nameUsage = (NameUsage) o;
    return key == nameUsage.key &&
        hidden == nameUsage.hidden &&
        Objects.equals(parentKey, nameUsage.parentKey) &&
        Objects.equals(rank, nameUsage.rank) &&
        Objects.equals(name, nameUsage.name) &&
        Objects.equals(comments, nameUsage.comments) &&
        Objects.equals(synonyms, nameUsage.synonyms) &&
        Objects.equals(vernacular, nameUsage.vernacular) &&
        Objects.equals(typeMaterial, nameUsage.typeMaterial);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, parentKey, hidden, rank, name, comments, synonyms, vernacular, typeMaterial);
  }
}
