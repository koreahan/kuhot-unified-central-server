// [서버] v3.183 helper: 로켓프레시/로켓직구는 표시용 배지 전용
function stripDeliveryBadgeForKey(v = '') {
  return String(v || '')
    .replace(/\s*\[\s*로켓\s*프레시[^\]]*\]\s*/giu, ' ')
    .replace(/\s*\[\s*로켓\s*직구[^\]]*\]\s*/giu, ' ')
    .replace(/\s*\[\s*쿠팡\s*직구[^\]]*\]\s*/giu, ' ')
    .replace(/\s*(?:로켓\s*프레시|로켓프레시|로켓\s*직구|로켓직구|쿠팡\s*직구|쿠팡직구|rocket\s*fresh|rocket\s*global)\s*/giu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function canonicalCollectorInput(input = {}) {
  return {
    ...input,
    title: stripDeliveryBadgeForKey(input.title || ''),
    option: stripDeliveryBadgeForKey(input.option || input.optionLine || ''),
  };
}

module.exports = { stripDeliveryBadgeForKey, canonicalCollectorInput };
