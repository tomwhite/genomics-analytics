import java.util.List;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.ga4gh.models.Call;
import org.ga4gh.models.FlatVariant;
import org.ga4gh.models.Variant;

class FlattenVariantFn extends DoFn<Variant, FlatVariant> {
  @Override
  public void process(Variant variant, Emitter<FlatVariant> emitter) {
    for (Call call : variant.getCalls()) {
      FlatVariant flatVariant = new FlatVariant();
      flatVariant.setId(variant.getId());
      flatVariant.setVariantSetId(variant.getVariantSetId());
      flatVariant.setNames1(get(variant.getNames(), 0));
      flatVariant.setNames2(get(variant.getNames(), 1));
      flatVariant.setCreated(variant.getCreated());
      flatVariant.setUpdated(variant.getUpdated());
      flatVariant.setReferenceName(variant.getReferenceName());
      flatVariant.setStart(variant.getStart());
      flatVariant.setEnd(variant.getEnd());
      flatVariant.setReferenceBases(variant.getReferenceBases());
      flatVariant.setAlternateBases1(get(variant.getAlternateBases(), 0));
      flatVariant.setAlternateBases2(get(variant.getAlternateBases(), 1));
      flatVariant.setAlleleIds1(get(variant.getAlleleIds(), 0));
      flatVariant.setAlleleIds2(get(variant.getAlleleIds(), 1));
      // variant.getInfo(); TODO: ignored for now
      flatVariant.setCallSetId(call.getCallSetId());
      flatVariant.setCallSetName(call.getCallSetName());
      flatVariant.setVariantId(call.getVariantId());
      flatVariant.setGenotype1(get(call.getGenotype(), 0));
      flatVariant.setGenotype2(get(call.getGenotype(), 1));
      // call.getPhaseset(); TODO: ignored for now
      flatVariant.setGenotypeLikelihood1(get(call.getGenotypeLikelihood(), 0));
      flatVariant.setGenotypeLikelihood2(get(call.getGenotypeLikelihood(), 1));
      //call.getInfo(); TODO: ignored for now
      emitter.emit(flatVariant);
    }
  }
  private static <T> T get(List<T> names, int index) {
    return index < names.size() ? names.get(index) : null;
  }
}
