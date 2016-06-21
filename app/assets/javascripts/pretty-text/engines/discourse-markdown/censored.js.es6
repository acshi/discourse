import { censor } from 'pretty-text/censored-words';

export function setup(helper) {
  return helper.addPreProcessor(censor);
}
